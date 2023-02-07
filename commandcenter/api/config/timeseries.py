import inspect

from starlette.config import Config

from commandcenter.timeseries import MongoTimeseriesHandler
from commandcenter.__version__ import __title__ as DATABASE_NAME



config = Config(".env")
handler_parameters = inspect.signature(MongoTimeseriesHandler).parameters


CC_TIMESERIES_DATABASE_NAME = config(
    "CC_TIMESERIES_DATABASE_NAME",
    default=handler_parameters["database_name"]
)
CC_TIMESERIES_COLLECTION_NAME = config(
    "CC_TIMESERIES_COLLECTION_NAME",
    default=handler_parameters["collection_name"]
)
CC_TIMESERIES_FLUSH_INTERVAL = config(
    "CC_TIMESERIES_FLUSH_INTERVAL",
    cast=float,
    default=handler_parameters["flush_interval"]
)
CC_TIMESERIES_BUFFER_SIZE = config(
    "CC_TIMESERIES_BUFFER_SIZE",
    cast=int,
    default=handler_parameters["buffer_size"]
)
CC_TIMESERIES_MAX_RETRIES = config(
    "CC_TIMESERIES_MAX_RETRIES",
    cast=int,
    default=handler_parameters["max_retries"]
)
CC_TIMESERIES_EXPIRE_AFTER = config(
    "CC_TIMESERIES_EXPIRE_AFTER",
    cast=int,
    default=handler_parameters["expire_after"]
)

CC_UNITOPS_DATABASE_NAME = config(
    "CC_UNITOPS_DATABASE_NAME",
    default=DATABASE_NAME
)
CC_UNITOPS_COLLECTION_NAME = config(
    "CC_UNITOPS_COLLECTION_NAME",
    default="unitops"
)