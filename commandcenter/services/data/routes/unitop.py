import logging
from typing import Dict, List

from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket
from fastapi.responses import JSONResponse
from sse_starlette import EventSourceResponse

from commandcenter.caching.tokens import ReferenceToken
from commandcenter.config.scopes import (
    CC_SCOPES_PIWEB_ACCESS,
    CC_SCOPES_PIWEB_ALLOW_ANY,
    CC_SCOPES_PIWEB_RAISE_ON_NONE,
    CC_SCOPES_TRAXX_ACCESS,
    CC_SCOPES_TRAXX_ALLOW_ANY,
    CC_SCOPES_TRAXX_RAISE_ON_NONE
)
from commandcenter.context import set_source
from commandcenter.dependencies import (
    get_cached_reference,
    get_manager,
    get_reference_token,
    requires
)
from commandcenter.exceptions import SubscriptionError
from commandcenter.integrations.models import (
    AnySubscriberMessage,
    AnySubscriptionRequest,
    BaseSubscriptionRequest
)
from commandcenter.integrations.protocols import Manager, Subscriber
from commandcenter.integrations.util import iter_subscribers
from commandcenter.sources import Sources
from commandcenter.util import sse_handler, ws_handler



_LOGGER = logging.getLogger("commandcenter.services.data.unitop")

router = APIRouter(prefix="/unitop", tags=["Unit Ops"])


REQUIRES = {
    Sources.PI_WEB_API: requires(
        scopes=list(CC_SCOPES_PIWEB_ACCESS),
        any_=CC_SCOPES_PIWEB_ALLOW_ANY,
        raise_on_no_scopes=CC_SCOPES_PIWEB_RAISE_ON_NONE
    ),
    Sources.TRAXX: requires(
        scopes=list(CC_SCOPES_TRAXX_ACCESS),
        any_=CC_SCOPES_TRAXX_ALLOW_ANY,
        raise_on_no_scopes=CC_SCOPES_TRAXX_RAISE_ON_NONE
    )
}


async def get_subscribers(
    subscriptions: AnySubscriptionRequest,
    request: Request
) -> List[Subscriber]:
    """Get subscribers from different sources."""
    groups = subscriptions.group()
    managers: Dict[Sources, Manager] = {}
    
    for source in groups.keys():
        # User must be authorized for subscriptions to all sources
        REQUIRES[source](request=request)
        with set_source(source):
            manager = await get_manager()
            managers[source] = manager
    
            subscribers: List[Subscriber] = []
    for source, subscriptions in groups.items():
        manager = managers[source]
        subscriber = await manager.subscribe(subscriptions)
        subscribers.append(subscriber)
    
    return subscribers

@router.post("/subscribe", response_model=ReferenceToken, dependencies=[Depends(requires())])
async def subscribe(
    token: ReferenceToken = Depends(
        get_reference_token(AnySubscriptionRequest)
    )
) -> ReferenceToken:
    """Generate a reference token to stream unit op data."""
    return token


@router.get("/stream/{token}", response_class=JSONResponse)
async def stream(
    request: Request,
    subscriptions: AnySubscriptionRequest = Depends(get_cached_reference(BaseSubscriptionRequest)),
) -> AnySubscriberMessage:
    """Submit a reference token to stream unit op data. This is an event sourcing
    (SSE) endpoint.
    """
    subscribers = await get_subscribers(
        subscriptions=subscriptions,
        request=request
    )
    send = iter_subscribers(*subscribers)
    iterble = sse_handler(send, _LOGGER)
    return EventSourceResponse(iterble)


@router.websocket("/stream/{token}/ws")
async def stream_ws(
    websocket: WebSocket,
    request: Request,
    subscriptions: AnySubscriptionRequest = Depends(
        get_cached_reference(
            BaseSubscriptionRequest,
            raise_on_miss=False
        )
    ),
) -> AnySubscriberMessage:
    """Submit a reference token to stream unit op data over the websocket protocol."""
    try:
        if not subscriptions:
            await websocket.close(code=1008, reason="Invalid token")
            return
        try:
            subscribers = await get_subscribers(
                subscriptions=subscriptions,
                request=request
            )
        except HTTPException as e:
            _LOGGER.info("Closing websocket due to failed dependency %r", e)
            await websocket.close(code=1006, reason="Failed dependency")
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
        for subscriber in subscribers: subscriber.stop()
        return
    send = iter_subscribers(*subscribers)
    await ws_handler(websocket, _LOGGER, None, send)