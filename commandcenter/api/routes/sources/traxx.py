import logging
from datetime import datetime
from typing import Type

from fastapi import APIRouter, Depends, Query, WebSocket
from fastapi.responses import StreamingResponse
from sse_starlette import EventSourceResponse

from commandcenter.api.config.scopes import (
    CC_SCOPES_TRAXX_ACCESS,
    CC_SCOPES_TRAXX_ALLOW_ANY,
    CC_SCOPES_TRAXX_RAISE_ON_NONE
)
from commandcenter.api.dependencies import (
    get_cached_reference,
    get_file_writer,
    get_manager,
    get_reference_token,
    get_traxx_http_client,
    parse_timestamp,
    requires,
    source
)
from commandcenter.auth import BaseUser
from commandcenter.caching.tokens import ReferenceToken
from commandcenter.integrations import Manager, SubscriptionError, iter_subscriber
from commandcenter.sources import Sources
from commandcenter.sources.traxx import (
    TraxxAPI,
    TraxxSubscriberMessage,
    TraxxSubscriptionRequest,
    get_sensor_data
)
from commandcenter.util import (
    FileWriter,
    chunked_transfer,
    format_timeseries_rows,
    sse_handler,
    ws_handler,
    TIMEZONE
)



_LOGGER = logging.getLogger("commandcenter.api.traxx")

router = APIRouter(
    prefix="/traxx",
    dependencies=[
        Depends(
            requires(
                scopes=list(CC_SCOPES_TRAXX_ACCESS),
                any_=CC_SCOPES_TRAXX_ALLOW_ANY,
                raise_on_no_scopes=CC_SCOPES_TRAXX_RAISE_ON_NONE
            )
        ),
        Depends(source(Sources.TRAXX))
    ],
    tags=["Traxx"]
)


@router.post("/subscribe", response_model=ReferenceToken)
async def subscribe(
    token: ReferenceToken = Depends(
        get_reference_token(TraxxSubscriptionRequest)
    )
) -> ReferenceToken:
    """Generate a reference token to stream Traxx data."""
    return token


@router.get("/stream/{token}", response_model=TraxxSubscriberMessage)
async def stream(
    subscriptions: TraxxSubscriptionRequest = Depends(get_cached_reference(TraxxSubscriptionRequest)),
    manager: Manager = Depends(get_manager)
) -> TraxxSubscriberMessage:
    """Submit a reference token to stream Traxx data. This is an event sourcing
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
            scopes=list(CC_SCOPES_TRAXX_ACCESS),
            any_=CC_SCOPES_TRAXX_ALLOW_ANY,
            raise_on_no_scopes=CC_SCOPES_TRAXX_RAISE_ON_NONE
        )
    ),
    __: Type[None] = Depends(source(Sources.PI_WEB_API)),
    subscriptions: TraxxSubscriptionRequest = Depends(
        get_cached_reference(
            TraxxSubscriptionRequest,
            raise_on_miss=False
        )
    )
) -> TraxxSubscriberMessage:
    """Submit a reference token to stream Traxx data over the websocket protocol."""
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


@router.get("/recorded/{token}")
async def recorded(
    subscriptions: TraxxSubscriptionRequest = Depends(get_cached_reference(TraxxSubscriptionRequest)),
    client: TraxxAPI = Depends(get_traxx_http_client),
    file_writer: FileWriter = Depends(get_file_writer),
    start_time: datetime = Depends(
        parse_timestamp(
            query=Query(
                default=None,
                alias="start_time",
                description="Start time for query. Defaults is -1h."
            ),
            default_timedelta=None
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
    timezone: str = Query(
        default=TIMEZONE,
        description=f"Timezone to convert data into. Defaults is application server timezone ({TIMEZONE})."
    )
):
    """Download a batch of recorded data. Supports .csv, .jsonl, and .ndjson"""
    send = get_sensor_data(
        client=client,
        subscriptions=subscriptions.subscriptions,
        start_time=start_time,
        end_time=end_time,
        timezone=timezone
    )
    
    buffer, writer, suffix, media_type = (
        file_writer.buffer, file_writer.writer, file_writer.buffer, file_writer.media_type
    )
    
    header = [subscription.key for subscription in sorted(subscriptions.subscriptions)]
    chunk_size = min(int(100_0000/len(header), 5000))
    writer(["timestamp", *header])
    filename = (
        f"{start_time.strftime('%Y%m%d%H%M%S')}-"
        f"{end_time.strftime('%Y%m%d%H%M%S')}-recorded.{suffix}"
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