import inspect

from starlette.config import Config
from starlette.datastructures import CommaSeparatedStrings, Secret

from commandcenter.core.sources.pi_web import PIChannelClient, WebIdType



config = Config(".env")
client_parameters = inspect.signature(PIChannelClient).parameters


# HTTP Client Params
CC_SOURCES_PIWEB_HTTP_BASE_URL = config(
    "CC_SOURCES_PIWEB_BASE_URL",
    default=""
)
CC_SOURCES_PIWEB_HTTP_MAX_CONNECTIONS = config(
    "CC_SOURCES_PIWEB_HTTP_MAX_CONNECTIONS",
    cast=int,
    default=25
)
CC_SOURCES_PIWEB_HTTP_REQUEST_TIMEOUT = config(
    "CC_SOURCES_PIWEB_HTTP_REQUEST_TIMEOUT",
    cast=float,
    default=10
)
CC_SOURCES_PIWEB_HTTP_KEEPALIVE_TIMEOUT = config(
    "CC_SOURCES_PIWEB_HTTP_KEEPALIVE_TIMEOUT",
    cast=float,
    default=20
)

# PI Authentication (Negotiate)
CC_SOURCES_PIWEB_AUTH_USERNAME = config(
    "CC_SOURCES_PIWEB_AUTH_USERNAME",
    default=None
)
CC_SOURCES_PIWEB_AUTH_PASSWORD = config(
    "CC_SOURCES_PIWEB_AUTH_PASSWORD",
    cast=Secret,
    default=None
)
CC_SOURCES_PIWEB_AUTH_DOMAIN = config(
    "CC_SOURCES_PIWEB_AUTH_DOMAIN",
    default=None
)
CC_SOURCES_PIWEB_AUTH_SERVICE = config(
    "CC_SOURCES_PIWEB_AUTH_SERVICE",
    default="HTTP"
)
CC_SOURCES_PIWEB_AUTH_DELEGATE = config(
    "CC_SOURCES_PIWEB_AUTH_DELEGATE",
    cast=bool,
    default=False
)
CC_SOURCES_PIWEB_AUTH_OPPORTUNISTIC = config(
    "CC_SOURCES_PIWEB_AUTH_OPPORTUNISTIC",
    cast=bool,
    default=False
)

# Channel Params
CC_SOURCES_PIWEB_CHANNEL_BASE_URL = config(
    "CC_SOURCES_PIWEB_CHANNEL_BASE_URL",
    default=""
)
CC_SOURCES_PIWEB_CHANNEL_WEB_ID_TYPE = config(
    "CC_SOURCES_PIWEB_CHANNEL_WEB_ID_TYPE",
    cast=lambda v: WebIdType(v),
    default=client_parameters["web_id_type"].default
)
CC_SOURCES_PIWEB_CHANNEL_MAX_CONNECTIONS = config(
    "CC_SOURCES_PIWEB_CHANNEL_MAX_CONNECTIONS",
    cast=int,
    default=client_parameters["max_connections"].default
)
CC_SOURCES_PIWEB_CHANNEL_MAX_SUBSCRIPTIONS = config(
    "CC_SOURCES_PIWEB_CHANNEL_MAX_SUBSCRIPTIONS",
    cast=int,
    default=client_parameters["max_subscriptions"].default
)
CC_SOURCES_PIWEB_CHANNEL_MAX_BUFFERED_MESSAGES = config(
    "CC_SOURCES_PIWEB_CHANNEL_MAX_BUFFERED_MESSAGES",
    cast=int,
    default=client_parameters["max_buffered_messages"].default
)
CC_SOURCES_PIWEB_CHANNEL_MAX_RECONNECT_ATTEMPTS = config(
    "CC_SOURCES_PIWEB_CHANNEL_MAX_RECONNECT_ATTEMPTS",
    cast=int,
    default=client_parameters["max_reconnect_attempts"].default
)
CC_SOURCES_PIWEB_CHANNEL_BACKOFF_FACTOR = config(
    "CC_SOURCES_PIWEB_CHANNEL_BACKOFF_FACTOR",
    cast=float,
    default=client_parameters["backoff_factor"].default
)
CC_SOURCES_PIWEB_CHANNEL_INITIAL_BACKOFF = config(
    "CC_SOURCES_PIWEB_CHANNEL_INITIAL_BACKOFF",
    cast=float,
    default=client_parameters["initial_backoff"].default
)
CC_SOURCES_PIWEB_CHANNEL_MAX_BACKOFF = config(
    "CC_SOURCES_PIWEB_CHANNEL_MAX_BACKOFF",
    cast=float,
    default=client_parameters["max_backoff"].default
)
CC_SOURCES_PIWEB_CHANNEL_PROTOCOLS = config(
    "CC_SOURCES_PIWEB_CHANNEL_PROTOCOLS",
    cast=CommaSeparatedStrings,
    default=client_parameters["protocols"].default
)
CC_SOURCES_PIWEB_CHANNEL_HEARTBEAT = config(
    "CC_SOURCES_PIWEB_CHANNEL_HEARTBEAT",
    cast=float,
    default=client_parameters["heartbeat"].default
)
CC_SOURCES_PIWEB_CHANNEL_CLOSE_TIMEOUT = config(
    "CC_SOURCES_PIWEB_CHANNEL_CLOSE_TIMEOUT",
    cast=float,
    default=client_parameters["close_timeout"].default
)
CC_SOURCES_PIWEB_CHANNEL_MAX_MESSAGE_SIZE = config(
    "CC_SOURCES_PIWEB_CHANNEL_MAX_MESSAGE_SIZE",
    cast=int,
    default=client_parameters["max_msg_size"].default
)