from fastapi import HTTPException, status

from commandcenter.core.integrations.models import (
    SubscriptionRequest,
    cache_subscription_request
)



# We want this to run in a threadpool because the cached requests are persisted
# to disk
def get_cached_subscription_request(key: str) -> SubscriptionRequest:
    """Dependency to recall a cached subscription request."""
    try:
        return cache_subscription_request(key)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Subscription key was not found or it has expired."
        )