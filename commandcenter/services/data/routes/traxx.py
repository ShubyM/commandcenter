import logging

from fastapi import APIRouter, Depends, HTTPException, WebSocket
from sse_starlette import EventSourceResponse

from commandcenter.common.events import subscriber_event_generator, websocket_event_generator
from commandcenter.config.scopes import (
    CC_SCOPES_TRAXX_ACCESS,
    CC_SCOPES_TRAXX_ALLOW_ANY,
    CC_SCOPES_TRAXX_RAISE_ON_NONE
)
from commandcenter.core.integrations.abc import AbstractManager
from commandcenter.core.integrations.exceptions import SubscriptionError
from commandcenter.core.sources import AvailableSources
from commandcenter.core.sources.traxx import (
    TraxxClient,
    TraxxSubscriberMessage,
    TraxxSubscriptionRequest,
    get_sensor_data
)
from commandcenter.core.util.cache import ReferenceToken
from commandcenter.dependencies import (
    SourceContext,
    get_cached_reference,
    get_manager,
    get_reference_token,
    get_traxx_http_client,
    requires
)
from commandcenter.sources import set_source



_LOGGER = logging.getLogger("commandcenter.services.data.traxx")


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
        Depends(SourceContext(AvailableSources.TRAXX))
    ],
    tags=["Traxx"]
)


@router.post("/subscribe", response_model=ReferenceToken)
async def subscribe(token: ReferenceToken = Depends(get_reference_token)) -> ReferenceToken:
    """Generate a subscription key to stream Traxx data."""
    return token


@router.get("/stream/{token}", response_model=TraxxSubscriberMessage)
async def stream(
    subscriptions: TraxxSubscriptionRequest = Depends(get_cached_reference(TraxxSubscriptionRequest)),
    manager: AbstractManager = Depends(get_manager)
) -> TraxxSubscriberMessage:
    """Stream PI data for a subscription key. This is an event sourcing (SSE) endpoint."""
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
        subscriptions: TraxxSubscriptionRequest = get_cached_reference(TraxxSubscriptionRequest)(token)
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