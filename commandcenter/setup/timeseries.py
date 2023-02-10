from commandcenter.config.mongo import (
    CC_MONGO_APPNAME,
    CC_MONGO_CONNECT_TIMEOUT,
    CC_MONGO_HEARTBEAT,
    CC_MONGO_MAX_POOL_SIZE,
    CC_MONGO_SERVER_SELECTION_TIMEOUT,
    CC_MONGO_TIMEOUT,
    CC_MONGO_URL
)
from commandcenter.config.timeseries import (
    CC_TIMESERIES_BUFFER_SIZE,
    CC_TIMESERIES_COLLECTION_NAME,
    CC_TIMESERIES_DATABASE_NAME,
    CC_TIMESERIES_EXPIRE_AFTER,
    CC_TIMESERIES_FLUSH_INTERVAL,
    CC_TIMESERIES_MAX_RETRIES
)
from commandcenter.caching import singleton
from commandcenter.timeseries import MongoTimeseriesHandler



@singleton
def configure_timeseries_handler() -> MongoTimeseriesHandler:
    """Configure a timeseries handler from the environment."""
    return MongoTimeseriesHandler(
        connection_url=CC_MONGO_URL,
        database_name=CC_TIMESERIES_DATABASE_NAME,
        collection_name=CC_TIMESERIES_COLLECTION_NAME,
        flush_interval=CC_TIMESERIES_FLUSH_INTERVAL,
        buffer_size=CC_TIMESERIES_BUFFER_SIZE,
        max_retries=CC_TIMESERIES_MAX_RETRIES,
        expire_after=CC_TIMESERIES_EXPIRE_AFTER,
        maxPoolSize=CC_MONGO_MAX_POOL_SIZE,
        connectTimeoutMS=CC_MONGO_CONNECT_TIMEOUT,
        heartbeatFrequencyMS=CC_MONGO_HEARTBEAT,
        serverSelectionTimeoutMS=CC_MONGO_SERVER_SELECTION_TIMEOUT,
        timeoutMS=CC_MONGO_TIMEOUT,
        appname=CC_MONGO_APPNAME
    )