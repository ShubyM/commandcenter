from fastapi import HTTPException, status

from commandcenter.core.integrations.models import (
    SubscriptionKey,
    SubscriptionRequest,
    cache_subscription_request
)



def get_cached_subscription_request(key: str) -> SubscriptionRequest:
    """Dependency to recall a cached subscription request."""
    try:
        return cache_subscription_request(key)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Subscription key was not found or it has expired."
        )


def get_subscription_key(subscriptions: SubscriptionRequest) -> SubscriptionKey:
    """Dependency to cache a subscription request and produce a subscription key."""
    key = subscriptions.key
    cache_subscription_request(key.key, subscriptions)
    return key