from fastapi import Depends

from commandcenter.common.cache import get_cached_reference
from commandcenter.config.integrations.collections import (
    CC_INTEGRATIONS_TIMESERIES_COLLECTION,
    CC_INTEGRATIONS_TIMESERIES_COLLECTION_DELTA
)
from commandcenter.core.integrations.abc import AbstractTimeseriesCollection
from commandcenter.core.integrations.collections import AvailableTimeseriesCollections
from commandcenter.core.integrations.models import BaseSubscriptionRequest
from commandcenter.core.timeseries import timeseries_collection



async def get_timeseries_collection(
    subscriptions: BaseSubscriptionRequest = Depends(get_cached_reference(BaseSubscriptionRequest))
) -> AbstractTimeseriesCollection:
    """Initialize a timseries collection from the environment configuration.
    
    This is intended to be used as a dependency.
    """
    collection = CC_INTEGRATIONS_TIMESERIES_COLLECTION
    if collection is AvailableTimeseriesCollections.LOCAL.cls:
        return collection(
            subscriptions.subscriptions,
            CC_INTEGRATIONS_TIMESERIES_COLLECTION_DELTA,
            timeseries_collection
        )
    raise RuntimeError("Received invalid collection.")