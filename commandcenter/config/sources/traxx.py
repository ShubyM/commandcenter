import inspect
import pathlib

from starlette.config import Config

from commandcenter.config import CC_HOME
from commandcenter.sources.traxx import TraxxClient



config = Config(".env")
client_parameters = inspect.signature(TraxxClient).parameters


# HTTP Params
CC_SOURCES_TRAXX_HTTP_BASE_URL = config(
    "CC_SOURCES_TRAXX_HTTP_BASE_URL",
    default=""
)
CC_SOURCES_TRAXX_HTTP_MAX_CONNECTIONS = config(
    "CC_SOURCES_TRAXX_HTTP_MAX_CONNECTIONS",
    cast=int,
    default=10
)
CC_SOURCES_TRAXX_HTTP_KEEPALIVE_TIMEOUT = config(
    "CC_SOURCES_TRAXX_HTTP_KEEPALIVE_TIMEOUT",
    cast=float,
    default=120
)
CC_SOURCES_TRAXX_HTTP_REQUEST_TIMEOUT = config(
    "CC_SOURCES_TRAXX_HTTP_REQUEST_TIMEOUT",
    cast=float,
    default=10
)

# Traxx Authentication
CC_SOURCES_TRAXX_AUTH_FILEPATH = config(
    "CC_SOURCES_TRAXX_AUTH_FILEPATH",
    cast=pathlib.Path,
    default=CC_HOME.joinpath(pathlib.Path("./integrations/sources/traxx/session.toml"))
)

# Stream Params
CC_SOURCES_TRAXX_STREAM_MAX_SUBSCRIPTIONS = config(
    "CC_SOURCES_TRAXX_STREAM_MAX_SUBSCRIPTIONS",
    cast=int,
    default=client_parameters["max_subscriptions"].default
)
CC_SOURCES_TRAXX_STREAM_MAX_BUFFERED_MESSAGES = config(
    "CC_SOURCES_TRAXX_STREAM_MAX_BUFFERED_MESSAGES",
    cast=int,
    default=client_parameters["max_buffered_messages"].default
)
CC_SOURCES_TRAXX_STREAM_UPDATE_INTERVAL = config(
    "CC_SOURCES_TRAXX_STREAM_UPDATE_INTERVAL",
    cast=float,
    default=client_parameters["update_interval"].default
)
CC_SOURCES_TRAXX_STREAM_MAX_MISSED_UPDATES = config(
    "CC_SOURCES_TRAXX_STREAM_MAX_MISSED_UPDATES",
    cast=float,
    default=client_parameters["max_missed_updates"].default
)
CC_SOURCES_TRAXX_STREAM_INITIAL_BACKOFF = config(
    "CC_SOURCES_TRAXX_STREAM_INITIAL_BACKOFF",
    cast=float,
    default=client_parameters["initial_backoff"].default
)