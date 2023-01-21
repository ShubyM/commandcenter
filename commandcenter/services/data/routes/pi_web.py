import asyncio
import concurrent.futures
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
    SubscriptionRequest,
    cache_subscription_request
)
from commandcenter.core.sources import AvailableSources
from commandcenter.core.sources.pi_web import PISubscription, PISubscriberMessage
from commandcenter.core.util.context import run_in_threadpool_executor_with_context
from commandcenter.dependencies import (
    SourceContext,
    get_cached_subscription_request,
    get_manager,
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
async def subscribe(subscriptions: PISubscriptionRequest) -> SubscriptionKey:
    """Generate a subscription key to stream PI data."""
    key = subscriptions.key
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        await run_in_threadpool_executor_with_context(
            executor,
            cache_subscription_request,
            key.key,
            subscriptions
        )
    return key


@router.get("/stream/{key}", response_model=PISubscriberMessage)
async def stream(
    subscriptions: PISubscriptionRequest = Depends(get_cached_subscription_request),
    manager: AbstractManager = Depends(get_manager)
) -> PISubscriberMessage:
    """Stream PI data for a subscription key. This is an event sourcing (SSE) endpoint."""
    iterator = subscriber_event_generator(manager, subscriptions.subscriptions)
    return EventSourceResponse(iterator)