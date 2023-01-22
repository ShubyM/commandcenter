import logging
from datetime import datetime, timedelta
from typing import List

import anyio
import pendulum
from dateutil.parser import parse
from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket
from fastapi.responses import StreamingResponse
from sse_starlette import EventSourceResponse

from commandcenter.common.events import (
    csv_write,
    jsonlines_write,
    subscriber_event_generator,
    timeseries_chunk_event_generator,
    websocket_event_generator
)
from commandcenter.config.scopes import (
    CC_SCOPES_PIWEB_ACCESS,
    CC_SCOPES_PIWEB_ALLOW_ANY,
    CC_SCOPES_PIWEB_RAISE_ON_NONE
)
from commandcenter.core.integrations.abc import AbstractManager
from commandcenter.core.integrations.exceptions import SubscriptionError
from commandcenter.core.integrations.util import TIMEZONE
from commandcenter.core.integrations.util.common import isoparse
from commandcenter.core.sources import AvailableSources
from commandcenter.core.sources.pi_web import (
    PIObjSearch,
    PIObjSearchRequest,
    PIObjSearchResult,
    PIObjType,
    PISubscriberMessage,
    PISubscriptionRequest,
    PIWebClient,
    WebIdType,
    get_interpolated,
    get_recorded,
    search_points
)
from commandcenter.core.util.cache import ReferenceToken, cache_by_token
from commandcenter.dependencies import (
    SourceContext,
    get_cached_reference,
    get_manager,
    get_pi_http_client,
    get_reference_token,
    requires
)
from commandcenter.sources import set_source



_LOGGER = logging.getLogger("commandcenter.services.data.pi_web")


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
        Depends(SourceContext(AvailableSources.PI_WEB_API))
    ],
    tags=["OSI PI"]
)


@router.post("/subscribe", response_model=ReferenceToken)
async def subscribe(key: ReferenceToken = Depends(get_reference_token(PISubscriptionRequest))) -> ReferenceToken:
    """Generate a reference token to stream PI data."""
    return key


@router.get("/stream/{token}", response_model=PISubscriberMessage)
async def stream(
    subscriptions: PISubscriptionRequest = Depends(get_cached_reference(PISubscriptionRequest)),
    manager: AbstractManager = Depends(get_manager)
) -> PISubscriberMessage:
    """Submit a subscription key to stream PI data. This is an event sourcing
    (SSE) endpoint.
    """
    subscriber = await manager.subscribe(subscriptions=subscriptions.subscriptions)
    iterator = subscriber_event_generator(subscriber)
    return EventSourceResponse(iterator)


@router.websocket("/stream/{token}/ws")
async def stream_ws(
    websocket: WebSocket,
    token: str
):
    """Submit a subscription key to stream PI data."""
    try:
        subscriptions: PISubscriptionRequest = get_cached_reference(PISubscriptionRequest)(token)
    except HTTPException:
        _LOGGER.info("Received invalid token")
        await websocket.close(code=1008, reason="Invalid token")
        raise
    with set_source(AvailableSources.PI_WEB_API):
        manager = await get_manager()
    try:
        try:
            subscriber = await manager.subscribe(subscriptions=subscriptions.subscriptions)
        except SubscriptionError:
            _LOGGER.warning("Refused connection due to a subscription error", exc_info=True)
            await websocket.close(code=1013)
        except Exception:
            # This may be due to a failed manager which we consider a server error
            _LOGGER.error("Refused connection due to server error", exc_info=True)
            await websocket.close(code=1011)
        else:
            await websocket.accept()
    except RuntimeError:
        # Websocket disconnected while we were subscribing
        subscriber.stop()
        return
    await websocket_event_generator(websocket, subscriber)


async def handle_search(client: PIWebClient, objsearch: PIObjSearchRequest) -> PIObjSearchResult:
    """Handles search for both GET and POST search methods."""
    # TODO: This method will need to get refactored when we have a more general
    # search strategy
    token = objsearch.token.token
    try:
        return await anyio.to_thread.run_sync(cache_by_token, token)
    except ValueError:
        pass
    points = [search_ for search_ in objsearch.search if search_.obj_type == PIObjType.POINT]
    result = await search_points(client, points)
    await anyio.to_thread.run_sync(cache_by_token, token, result)
    return result


@router.post("/search", response_model=PIObjSearchResult)
async def search(
    objsearch: PIObjSearchRequest,
    client: PIWebClient = Depends(get_pi_http_client)
) -> PIObjSearchResult:
    """Search for a collection of PI objects to generate subscriptions."""
    return await handle_search(client, objsearch)


@router.get("/points", response_model=PIObjSearchResult)
async def points(
    client: PIWebClient = Depends(get_pi_http_client),
    name: List[str] = Query(default=...),
    web_id_type: str = WebIdType.FULL.value,
    obj_type: str = PIObjType.POINT.value,
    server: str = None,
    database: str = None
) -> PIObjSearchResult:
    """Search for a collection of PI objects to generate subscriptions."""
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


@router.get("/interpolated/{token}/json")
async def recorded(
    subscriptions: PISubscriptionRequest = Depends(get_cached_reference(PISubscriptionRequest)),
    client: PIWebClient = Depends(get_pi_http_client),
    start_time: str = Query(default=None, description="Start time for query. Defaults to -1h."),
    end_time: str = Query(default=None, description="End time for query. Defaults to current time."),
    interval: int = Query(default=60, description="Time interval in seconds."),
    request_chunk_size: int = Query(default=5000, description="Max rows to return for single PI Web request."),
    timezone: str = Query(default=TIMEZONE, description=f"Timezone to convert data into. Defaults to application server timezone ({TIMEZONE}).")
):
    start_time = parse(start_time) if start_time else datetime.now()-timedelta(hours=1)
    end_time = parse(end_time) if end_time else end_time
    iterator = get_interpolated(
        client,
        subscriptions.subscriptions,
        start_time,
        end_time,
        interval,
        request_chunk_size,
        timezone
    )
    try:
        header = await iterator.__anext__()
    except StopAsyncIteration:
        # Something failed
        raise RuntimeError("Timseries iterator exhausted.")
    
    if not len(header) == len(subscriptions.subscriptions):
        raise ValueError("Header row does not match number of subscriptions.")
    
    writer, buffer = jsonlines_write()

    writer(["Timestamp", *header])

    return StreamingResponse(
        timeseries_chunk_event_generator(
            iterator,
            buffer,
            writer
        ),
        media_type="application/json"
    )
    