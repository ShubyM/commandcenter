import logging
from datetime import datetime, timedelta
from typing import List, Type

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket
from fastapi.responses import StreamingResponse
from sse_starlette import EventSourceResponse

import anyio
from commandcenter.auth import BaseUser
from commandcenter.caching.tokens import ReferenceToken
from commandcenter.config.scopes import (
    CC_SCOPES_PIWEB_ACCESS,
    CC_SCOPES_PIWEB_ALLOW_ANY,
    CC_SCOPES_PIWEB_RAISE_ON_NONE
)
from commandcenter.dependencies import (
    get_cached_reference,
    get_file_writer,
    get_manager,
    get_pi_http_client,
    get_reference_token,
    parse_timestamp,
    requires,
    source
)
from commandcenter.exceptions import SubscriptionError
from commandcenter.integrations.base import iter_subscriber
from commandcenter.integrations.protocols import Manager
from commandcenter.sources import Sources
from commandcenter.sources.pi_web import (
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
from commandcenter.util import sse_handler, ws_handler



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
        Depends(source(Sources.PI_WEB_API))
    ],
    tags=["OSI PI"]
)


@router.post("/subscribe", response_model=ReferenceToken)
async def subscribe(
    key: ReferenceToken = Depends(
        get_reference_token(PISubscriptionRequest)
    )
) -> ReferenceToken:
    """Generate a reference token to stream PI data."""
    return key


@router.get("/stream/{token}", response_model=PISubscriberMessage)
async def stream(
    subscriptions: PISubscriptionRequest = Depends(get_cached_reference(PISubscriptionRequest)),
    manager: Manager = Depends(get_manager)
) -> PISubscriberMessage:
    """Submit a subscription key to stream PI data. This is an event sourcing
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
    ),
    manager: Manager = Depends(get_manager)
):
    """Submit a subscription key to stream PI data over the websocket protocol."""
    if not subscriptions:
        await websocket.close(code=1008, reason="Invalid token")
        return
    try:
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


async def handle_search(client: PIWebClient, objsearch: PIObjSearchRequest) -> PIObjSearchResult:
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
    _ = await anyio.to_thread.run_sync(get_reference_token, result)
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