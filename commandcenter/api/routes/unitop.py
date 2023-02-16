import logging
from datetime import datetime
from typing import List

import anyio
from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, status
from fastapi.responses import JSONResponse, StreamingResponse
from motor.motor_asyncio import AsyncIOMotorCollection
from sse_starlette import EventSourceResponse

from commandcenter.config.scopes import (
    COMMANDCENTER_READ_ACCESS,
    COMMANDCENTER_READ_ALLOW_ANY,
    COMMANDCENTER_READ_RAISE_ON_NONE,
    COMMANDCENTER_WRITE_ACCESS,
    COMMANDCENTER_WRITE_ALLOW_ANY,
    COMMANDCENTER_WRITE_RAISE_ON_NONE
)
from commandcenter.dependencies import (
    get_file_writer,
    get_timeseries_collection,
    get_timeseries_handler,
    get_unitop,
    get_unitop_and_authorize,
    get_unitop_collection,
    get_unitop_subscribers,
    get_unitops,
    parse_timestamp,
    requires
)
from commandcenter.auth import BaseUser
from commandcenter.integrations import (
    AnySubscriberMessage,
    Subscriber,
    iter_subscribers
)
from commandcenter.timeseries import (
    MongoTimeseriesHandler,
    TimeseriesSamples,
    UnitOp,
    UnitOpQueryResult,
    get_timeseries
)
from commandcenter.util import (
    FileWriter,
    Status,
    StatusOptions,
    chunked_transfer,
    format_timeseries_rows,
    sse_handler,
    ws_handler
)



_LOGGER = logging.getLogger("commandcenter.api.unitop")

router = APIRouter(
    prefix="/unitop",
    dependencies=[
        Depends(
            requires(
                scopes=COMMANDCENTER_READ_ACCESS,
                any_=COMMANDCENTER_READ_ALLOW_ANY,
                raise_on_no_scopes=COMMANDCENTER_READ_RAISE_ON_NONE
            )
        )
    ],
    tags=["Unit Ops"]
)


@router.get("/search/{unitop_id}", response_model=UnitOp)
async def unitop(
    unitop: UnitOp = Depends(get_unitop)
) -> UnitOp:
    """Retrieve a unitop record."""
    return unitop


@router.get("/search", response_model=UnitOpQueryResult)
async def unitops(
    unitops: UnitOpQueryResult = Depends(get_unitops)
) -> UnitOpQueryResult:
    """Retrieve a collection of unitop records."""
    return unitops


@router.post(
    "/save",
    response_model=Status,
    dependencies=[
        Depends(
            requires(
                scopes=COMMANDCENTER_WRITE_ACCESS,
                any_=COMMANDCENTER_WRITE_ALLOW_ANY,
                raise_on_no_scopes=COMMANDCENTER_WRITE_RAISE_ON_NONE
            )
        )
    ]
)
async def save(
    unitop: UnitOp,
    collection: AsyncIOMotorCollection = Depends(get_unitop_collection)
) -> Status:
    """Save a unitop to the database."""
    result = await collection.replace_one(
        {"unitop_id": unitop.unitop_id},
        unitop.dict(),
        upsert=True
    )
    if result.modified_count > 0 or result.matched_count > 0 or result.upserted_id is not None:
        return Status(status=StatusOptions.OK)
    return Status(status=StatusOptions.FAILED)


@router.get("/stream/{unitop_id}", response_class=JSONResponse)
async def stream(
    subscribers: List[Subscriber] = Depends(get_unitop_subscribers)
) -> AnySubscriberMessage:
    """Stream data for a unitop. This is an event sourcing (SSE) endpoint."""
    send = iter_subscribers(*subscribers)
    iterble = sse_handler(send, _LOGGER)
    return EventSourceResponse(iterble)


@router.websocket("/stream/{unitop_id}/ws")
async def stream_ws(
    websocket: WebSocket,
    _: BaseUser = Depends(
        requires(
            scopes=COMMANDCENTER_READ_ACCESS,
            any_=COMMANDCENTER_READ_ALLOW_ANY,
            raise_on_no_scopes=COMMANDCENTER_READ_RAISE_ON_NONE
        )
    ),
    subscribers: List[Subscriber] = Depends(get_unitop_subscribers)
) -> AnySubscriberMessage:
    """Stream data for a unitop over the websocket protocol."""
    try:
        await websocket.accept()
    except RuntimeError:
        # Websocket disconnected while we were subscribing
        for subscriber in subscribers: subscriber.stop()
        return
    send = iter_subscribers(*subscribers)
    await ws_handler(websocket, _LOGGER, None, send)


@router.post(
    "/samples/publish",
    response_model=Status,
    dependencies=[
        Depends(
            requires(
                scopes=COMMANDCENTER_WRITE_ACCESS,
                any_=COMMANDCENTER_WRITE_ALLOW_ANY,
                raise_on_no_scopes=COMMANDCENTER_WRITE_RAISE_ON_NONE
            )
        )
    ]
)
async def samples(
    samples: TimeseriesSamples,
    handler: MongoTimeseriesHandler = Depends(get_timeseries_handler)
) -> Status:
    """Send timeseries data to the API."""
    try:
        await anyio.to_thread.run_sync(handler.publish(samples))
    except TimeoutError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to process samples."
        )
    return Status(status=StatusOptions.OK)


@router.get("samples/{unitop_id}", response_class=StreamingResponse)
async def recorded(
    collection: AsyncIOMotorCollection = Depends(get_timeseries_collection),
    unitop: UnitOp = Depends(get_unitop_and_authorize),
    file_writer: FileWriter = Depends(get_file_writer),
    start_time: datetime = Depends(
        parse_timestamp(
            query=Query(
                default=None,
                alias="start_time",
                description="Start time for query. Default is -1h."
            ),
            default_timedelta=3600
        )
    ),
    end_time: datetime = Depends(
        parse_timestamp(
            query=Query(
                default=None,
                alias="end_time",
                description="End time for query. Default is current time."
            ),
            default_timedelta=None
        )
    ),
    scan_rate: int = Query(
        default=5,
        description="The update frequency of the data. Default is 5 seconds. You do "
            "not need to know this value exactly. A representative number on the "
            "correct order of magnitide is sufficient."
    )
) -> StreamingResponse:
    """Download a batch of recorded data. Supports .csv, .jsonl, and .ndjson"""
    send = get_timeseries(
        collection=collection,
        subscriptions=list(unitop.data_mapping.values()),
        start_time=start_time,
        end_time=end_time,
        scan_rate=scan_rate
    )

    buffer, writer, suffix, media_type = (
        file_writer.buffer, file_writer.writer, file_writer.suffix, file_writer.media_type
    )
    
    header = [item[0] for item in sorted(unitop.data_mapping.items(), key=lambda x: x[1])]
    chunk_size = min(int(100_0000/len(header)), 5000)
    writer(["timestamp", *header])
    filename = (
        f"{start_time.strftime('%Y%m%d%H%M%S')}-"
        f"{end_time.strftime('%Y%m%d%H%M%S')}-{unitop.unitop_id}.{suffix}"
    )
    return StreamingResponse(
        chunked_transfer(
            send=send,
            buffer=buffer,
            writer=writer,
            formatter=format_timeseries_rows,
            logger=_LOGGER,
            chunk_size=chunk_size
        ),
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )