from datetime import timedelta

from starlette.config import Config

from commandcenter.core.integrations.collections import AvailableTimeseriesCollections



config = Config(".env")


CC_INTEGRATIONS_TIMESERIES_COLLECTION = config(
    "CC_INTEGRATIONS_TIMESERIES_COLLECTION",
    cast=lambda v: AvailableTimeseriesCollections(v).cls,
    default=AvailableTimeseriesCollections.DEFAULT.value
)
CC_INTEGRATIONS_TIMESERIES_COLLECTION_DELTA = config(
    "CC_INTEGRATIONS_TIMESERIES_COLLECTION_DELTA",
    cast=lambda v: timedelta(minutes=v),
    default=120
)