import logging
from datetime import datetime
from typing import List, Type

from fastapi import APIRouter, Depends, Query, WebSocket
from fastapi.responses import StreamingResponse
from sse_starlette import EventSourceResponse

import anyio
from commandcenter.api.config.scopes import (
    CC_SCOPES_PIWEB_ACCESS,
    CC_SCOPES_PIWEB_ALLOW_ANY,
    CC_SCOPES_PIWEB_RAISE_ON_NONE
)
from commandcenter.api.dependencies import (
    get_cached_reference,
    get_file_writer,
    get_manager,
    get_pi_http_client,
    get_reference_token,
    parse_timestamp,
    requires,
    source
)
from commandcenter.auth import BaseUser
from commandcenter.caching.tokens import ReferenceToken
from commandcenter.integrations import Manager, SubscriptionError, iter_subscriber
from commandcenter.sources import Sources
from commandcenter.sources.pi_web import (
    PIObjSearch,
    PIObjSearchRequest,
    PIObjSearchResult,
    PIObjType,
    PISubscriberMessage,
    PISubscriptionRequest,
    PIWebAPI,
    WebIdType,
    get_interpolated,
    get_recorded,
    search_points
)
from commandcenter.util import (
    FileWriter,
    chunked_transfer,
    format_timeseries_rows,
    sse_handler,
    ws_handler,
    TIMEZONE
)



_LOGGER = logging.getLogger("commandcenter.api.pi_web")


router = APIRouter(
    prefix="/piweb",
    dependencies=[
        Depends(
            requires(
                scopes=list(CC_SCOPES_PIWEB_ACCESS),
                any_=CC_SCOPES_PIWEB_ALLOW_ANY,
                raise_on_no_scopes=CC_SCOPES_PIWEB_RAISE_ON_NONE
            )
        ),
        Depends(source(Sources.PI_WEB_API))
    ],
    tags=["OSI PI"]
)


async def handle_search(client: PIWebAPI, objsearch: PIObjSearchRequest) -> PIObjSearchResult:
    """Handles search for both GET and POST search methods."""
    # TODO: This method will need to get refactored when we have a more general
    # search strategy
    token = objsearch.token.token
    result = await anyio.to_thread.run_sync(
        get_cached_reference(PIObjSearchResult, raise_on_miss=False),
        token
    )
    if result:
        return result
    points = [search_ for search_ in objsearch.search if search_.obj_type == PIObjType.POINT]
    result = await search_points(client, points)
    _ = await anyio.to_thread.run_sync(get_reference_token(PIObjSearchRequest), result)
    return result


@router.post("/subscribe", response_model=ReferenceToken)
async def subscribe(
    token: ReferenceToken = Depends(
        get_reference_token(PISubscriptionRequest)
    )
) -> ReferenceToken:
    """Generate a reference token to stream PI data."""
    return token


@router.get("/stream/{token}", response_model=PISubscriberMessage)
async def stream(
    subscriptions: PISubscriptionRequest = Depends(get_cached_reference(PISubscriptionRequest)),
    manager: Manager = Depends(get_manager)
) -> PISubscriberMessage:
    """Submit a reference token to stream PI data. This is an event sourcing
    (SSE) endpoint.
    """
    subscriber = await manager.subscribe(subscriptions=subscriptions.subscriptions)
    send = iter_subscriber(subscriber)
    iterble = sse_handler(send, _LOGGER)
    return EventSourceResponse(iterble)


@router.websocket("/stream/{token}/ws")
async def stream_ws(
    websocket: WebSocket,
    _: BaseUser = Depends(
        requires(
            scopes=list(CC_SCOPES_PIWEB_ACCESS),
            any_=CC_SCOPES_PIWEB_ALLOW_ANY,
            raise_on_no_scopes=CC_SCOPES_PIWEB_RAISE_ON_NONE
        )
    ),
    __: Type[None] = Depends(source(Sources.PI_WEB_API)),
    subscriptions: PISubscriptionRequest = Depends(
        get_cached_reference(
            PISubscriptionRequest,
            raise_on_miss=False
        )
    )
) -> PISubscriberMessage:
    """Submit a reference token to stream PI data over the websocket protocol."""
    manager = await get_manager()
    try:
        if not subscriptions:
            await websocket.close(code=1008, reason="Invalid token")
            return
        try:
            subscriber = await manager.subscribe(subscriptions=subscriptions.subscriptions)
        except SubscriptionError:
            _LOGGER.warning("Refused connection due to a subscription error", exc_info=True)
            await websocket.close(code=1013)
            return
        except Exception:
            _LOGGER.error("Refused connection due to server error", exc_info=True)
            await websocket.close(code=1011)
            return
        else:
            await websocket.accept()
    except RuntimeError:
        # Websocket disconnected while we were subscribing
        subscriber.stop()
        return
    send = iter_subscriber(subscriber)
    await ws_handler(websocket, _LOGGER, None, send)


@router.post("/search", response_model=PIObjSearchResult)
async def search(
    objsearch: PIObjSearchRequest,
    client: PIWebAPI = Depends(get_pi_http_client)
) -> PIObjSearchResult:
    """Search for a collection of PI objects to generate subscriptions."""
    return await handle_search(client, objsearch)


@router.get("/points", response_model=PIObjSearchResult)
async def points(
    client: PIWebAPI = Depends(get_pi_http_client),
    name: List[str] = Query(
        default=...,
        description="Name of PI points. Multiple names may be specified with "
        "multiple instances of the parameter"
    ),
    web_id_type: str = Query(
        default=WebIdType.FULL.value,
        description="Used to specify the type of WebID. Useful for URL brevity "
        "and other special cases. Default is FULL"
    ),
    obj_type: str = Query(
        default=PIObjType.POINT.value,
        description="The PI object type to search. Can be element | attribute | "
        "point. Only 'point' is currently supported."
    ),
    server: str = None,
    database: str = None
) -> PIObjSearchResult:
    """Search for a collection of PI points to generate subscriptions."""
    objsearch = PIObjSearchRequest(
        search=[
            PIObjSearch(
                search=name_,
                web_id_type=web_id_type,
                obj_type=obj_type,
                server=server,
                database=database
            ) for name_ in name
        ]
    )
    return await handle_search(client, objsearch)


@router.get("/interpolated/{token}")
async def interpolated(
    subscriptions: PISubscriptionRequest = Depends(get_cached_reference(PISubscriptionRequest)),
    client: PIWebAPI = Depends(get_pi_http_client),
    file_writer: FileWriter = Depends(get_file_writer),
    start_time: datetime = Depends(
        parse_timestamp(
            query=Query(
                default=None,
                alias="start_time",
                description="Start time for query. Defaults is -1h."
            ),
            default_timedelta=3600
        )
    ),
    end_time: datetime = Depends(
        parse_timestamp(
            query=Query(
                default=None,
                alias="end_time",
                description="End time for query. Defaults is current time."
            ),
            default_timedelta=None
        )
    ),
    interval: int = Query(
        default=60,
        description="Time interval in seconds. Defaults is 60 seconds."
    ),
    timezone: str = Query(
        default=TIMEZONE,
        description=f"Timezone to convert data into. Defaults is application server timezone ({TIMEZONE})."
    )
):
    """Download a batch of interpolated data. Supports .csv, .jsonl, and .ndjson"""
    send = get_interpolated(
        client=client,
        subscriptions=subscriptions.subscriptions,
        start_time=start_time,
        end_time=end_time,
        interval=interval,
        timezone=timezone
    )
    
    buffer, writer, suffix, media_type = (
        file_writer.buffer, file_writer.writer, file_writer.suffix, file_writer.media_type
    )
    
    header = [subscription.name for subscription in sorted(subscriptions.subscriptions)]
    chunk_size = min(int(100_0000/len(header)), 5000)
    writer(["timestamp", *header])
    filename = (
        f"{start_time.strftime('%Y%m%d%H%M%S')}-"
        f"{end_time.strftime('%Y%m%d%H%M%S')}-pi-interpolated.{suffix}"
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


@router.get("/recorded/{token}")
async def recorded(
    subscriptions: PISubscriptionRequest = Depends(get_cached_reference(PISubscriptionRequest)),
    client: PIWebAPI = Depends(get_pi_http_client),
    file_writer: FileWriter = Depends(get_file_writer),
    start_time: datetime = Depends(
        parse_timestamp(
            query=Query(
                default=None,
                alias="start_time",
                description="Start time for query. Defaults is -1h."
            ),
            default_timedelta=3600
        )
    ),
    end_time: datetime = Depends(
        parse_timestamp(
            query=Query(
                default=None,
                alias="end_time",
                description="End time for query. Defaults is current time."
            ),
            default_timedelta=None
        )
    ),
    scan_rate: int = Query(
        default=5,
        description="The scan rate of the PI server. Defaults is 5 seconds. You do "
            "not need to know this value exactly. A representative number on the "
            "correct order of magnitide is sufficient."
    ),
    timezone: str = Query(
        default=TIMEZONE,
        description=f"Timezone to convert data into. Defaults is application server timezone ({TIMEZONE})."
    )
):
    """Download a batch of recorded data. Supports .csv, .jsonl, and .ndjson"""
    send = get_recorded(
        client=client,
        subscriptions=subscriptions.subscriptions,
        start_time=start_time,
        end_time=end_time,
        scan_rate=scan_rate,
        timezone=timezone
    )
    
    buffer, writer, suffix, media_type = (
        file_writer.buffer, file_writer.writer, file_writer.suffix, file_writer.media_type
    )
    
    header = [subscription.name for subscription in sorted(subscriptions.subscriptions)]
    chunk_size = min(int(100_0000/len(header)), 5000)
    writer(["timestamp", *header])
    filename = (
        f"{start_time.strftime('%Y%m%d%H%M%S')}-"
        f"{end_time.strftime('%Y%m%d%H%M%S')}-pi-recorded.{suffix}"
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