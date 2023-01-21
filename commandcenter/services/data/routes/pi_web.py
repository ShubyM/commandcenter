from typing import List

from fastapi import APIRouter, Depends
from sse_starlette import EventSourceResponse

from commandcenter.common.events import subscriber_event_generator
from commandcenter.config.scopes import (
    CC_SCOPES_PIWEB_ACCESS,
    CC_SCOPES_PIWEB_ALLOW_ANY,
    CC_SCOPES_PIWEB_RAISE_ON_NONE
)
from commandcenter.core.integrations.abc import AbstractManager
from commandcenter.core.integrations.models import (
    SubscriptionKey,
    SubscriptionRequest
)
from commandcenter.core.sources import AvailableSources
from commandcenter.core.sources.pi_web import (
    PISubscription,
    PISubscriberMessage,
    get_interpolated,
    get_recorded,
    search_points
)
from commandcenter.dependencies import (
    SourceContext,
    get_cached_subscription_request,
    get_manager,
    get_pi_http_client,
    get_subscription_key,
    requires
)



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


class PISubscriptionRequest(SubscriptionRequest):
    """Model for PI Web subscription requests."""
    subscriptions: List[PISubscription]


@router.post("/subscribe", response_model=SubscriptionKey)
async def subscribe(key: SubscriptionKey = Depends(get_subscription_key)) -> SubscriptionKey:
    """Generate a subscription key to stream PI data."""
    return key


@router.get("/stream/{key}", response_model=PISubscriberMessage)
async def stream(
    subscriptions: PISubscriptionRequest = Depends(get_cached_subscription_request),
    manager: AbstractManager = Depends(get_manager)
) -> PISubscriberMessage:
    """Submit a subscription key to stream PI data. This is an event sourcing
    (SSE) endpoint.
    """
    iterator = subscriber_event_generator(manager, subscriptions.subscriptions)
    return EventSourceResponse(iterator)


@router.post("/search")
async def search(
    t
):
    pass