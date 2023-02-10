from starlette.config import Config



config = Config(".env")


CC_MEMCACHED_URL = config(
    "CC_MEMCACHED_URL",
    default="localhost:11211"
)
CC_MEMCACHED_CONNECT_TIMEOUT = config(
    "CC_MEMCACHED_CONNECT_TIMEOUT",
    cast=float,
    default=10
)
CC_MEMCACHED_TIMEOUT = config(
    "CC_MEMCACHED_TIMEOUT",
    cast=float,
    default=10
)
CC_MEMCACHED_MAX_CONNECTIONS = config(
    "CC_MEMCACHED_MAX_CONNECTIONS",
    cast=int,
    default=4
)