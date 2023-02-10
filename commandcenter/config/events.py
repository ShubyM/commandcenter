import inspect

from starlette.config import Config

from commandcenter.events import EventBus, MongoEventHandler
from commandcenter.__version__ import __title__ as DATABASE_NAME



config = Config(".env")
bus_parameters = inspect.signature(EventBus).parameters
handler_parameters = inspect.signature(MongoEventHandler).parameters


CC_EVENTS_BUS_EXCHANGE = config(
    "CC_EVENTS_BUS_EXCHANGE",
    default="commandcenter-events"
)
CC_EVENTS_BUS_MAX_SUBSCRIBERS = config(
    "CC_EVENTS_BUS_MAX_SUBSCRIBERS",
    cast=int,
    default=bus_parameters["max_subscribers"].default
)
CC_EVENTS_BUS_SUBSCRIBER_MAXLEN = config(
    "CC_EVENTS_BUS_SUBSCRIBER_MAXLEN",
    cast=int,
    default=bus_parameters["maxlen"].default
)
CC_EVENTS_BUS_TIMEOUT = config(
    "CC_EVENTS_BUS_TIMEOUT",
    cast=float,
    default=bus_parameters["timeout"].default
)
CC_EVENTS_BUS_RECONNECT_TIMEOUT = config(
    "CC_EVENTS_BUS_RECONNECT_TIMEOUT",
    cast=float,
    default=bus_parameters["reconnect_timeout"].default
)
CC_EVENTS_BUS_MAX_BACKOFF = config(
    "CC_EVENTS_BUS_MAX_BACKOFF",
    cast=float,
    default=bus_parameters["max_backoff"].default
)
CC_EVENTS_BUS_INITIAL_BACKOFF = config(
    "CC_EVENTS_BUS_INITIAL_BACKOFF",
    cast=float,
    default=bus_parameters["initial_backoff"].default
)

CC_EVENTS_DATABASE_NAME = config(
    "CC_EVENTS_DATABASE_NAME",
    default=DATABASE_NAME
)
CC_EVENTS_COLLECTION_NAME = config(
    "CC_EVENTS_COLLECTION_NAME",
    default="events"
)
CC_EVENTS_FLUSH_INTERVAL = config(
    "CC_EVENTS_FLUSH_INTERVAL",
    cast=float,
    default=handler_parameters["flush_interval"].default
)
CC_EVENTS_BUFFER_SIZE = config(
    "CC_EVENTS_BUFFER_SIZE",
    cast=int,
    default=handler_parameters["buffer_size"].default
)
CC_EVENTS_MAX_RETRIES = config(
    "CC_EVENTS_MAX_RETRIES",
    cast=int,
    default=handler_parameters["max_retries"].default
)
CC_EVENTS_EXPIRE_AFTER = config(
    "CC_EVENTS_EXPIRE_AFTER",
    cast=int,
    default=handler_parameters["expire_after"].default
)

CC_TOPICS_DATABASE_NAME = config(
    "CC_TOPICS_DATABASE_NAME",
    default=DATABASE_NAME
)
CC_TOPICS_COLLECTION_NAME = config(
    "CC_TOPICS_COLLECTION_NAME",
    default="topics"
)