from commandcenter.api.config.mongo import (
    CC_MONGO_APPNAME,
    CC_MONGO_CONNECT_TIMEOUT,
    CC_MONGO_HEARTBEAT,
    CC_MONGO_MAX_POOL_SIZE,
    CC_MONGO_SERVER_SELECTION_TIMEOUT,
    CC_MONGO_TIMEOUT,
    CC_MONGO_URL
)
from commandcenter.api.config.events import (
    CC_EVENTS_BUS_EXCHANGE,
    CC_EVENTS_BUS_MAX_BACKOFF,
    CC_EVENTS_BUS_MAX_SUBSCRIBERS,
    CC_EVENTS_BUS_INITIAL_BACKOFF,
    CC_EVENTS_BUS_RECONNECT_TIMEOUT,
    CC_EVENTS_BUS_SUBSCRIBER_MAXLEN,
    CC_EVENTS_BUS_TIMEOUT,
    CC_EVENTS_BUFFER_SIZE,
    CC_EVENTS_COLLECTION_NAME,
    CC_EVENTS_DATABASE_NAME,
    CC_EVENTS_EXPIRE_AFTER,
    CC_EVENTS_FLUSH_INTERVAL,
    CC_EVENTS_MAX_RETRIES
)
from commandcenter.api.setup.rabbitmq import setup_rabbitmq
from commandcenter.caching import singleton
from commandcenter.events import EventBus, MongoEventHandler



@singleton
def setup_event_handler() -> MongoEventHandler:
    """Configure an event handler from the environment."""
    return MongoEventHandler(
        connection_url=CC_MONGO_URL,
        database_name=CC_EVENTS_DATABASE_NAME,
        collection_name=CC_EVENTS_COLLECTION_NAME,
        flush_interval=CC_EVENTS_FLUSH_INTERVAL,
        buffer_size=CC_EVENTS_BUFFER_SIZE,
        max_retries=CC_EVENTS_MAX_RETRIES,
        expire_after=CC_EVENTS_EXPIRE_AFTER,
        maxPoolSize=CC_MONGO_MAX_POOL_SIZE,
        connectTimeoutMS=CC_MONGO_CONNECT_TIMEOUT,
        heartbeatFrequencyMS=CC_MONGO_HEARTBEAT,
        serverSelectionTimeoutMS=CC_MONGO_SERVER_SELECTION_TIMEOUT,
        timeoutMS=CC_MONGO_TIMEOUT,
        appname=CC_MONGO_APPNAME
    )


@singleton
def setup_event_bus() -> EventBus:
    """Configure an event bus from the environment.
    
    This must be run in the same thread as the event loop.
    """
    factory = setup_rabbitmq()
    return EventBus(
        factory=factory,
        exchange=CC_EVENTS_BUS_EXCHANGE,
        max_subscribers=CC_EVENTS_BUS_MAX_SUBSCRIBERS,
        maxlen=CC_EVENTS_BUS_SUBSCRIBER_MAXLEN,
        timeout=CC_EVENTS_BUS_TIMEOUT,
        reconnect_timeout=CC_EVENTS_BUS_RECONNECT_TIMEOUT,
        max_backoff=CC_EVENTS_BUS_MAX_BACKOFF,
        initial_backoff=CC_EVENTS_BUS_INITIAL_BACKOFF
    )