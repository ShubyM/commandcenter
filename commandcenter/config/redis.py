from starlette.config import Config



config = Config(".env")


CC_REDIS_URL = config(
    "CC_REDIS_URL",
    default="redis://localhost:6379/0"
)
CC_REDIS_CONNECT_TIMEOUT = config(
    "CC_REDIS_CONNECT_TIMEOUT",
    cast=float,
    default=10
)
CC_REDIS_TIMEOUT = config(
    "CC_REDIS_TIMEOUT",
    cast=float,
    default=10
)
CC_REDIS_MAX_CONNECTIONS = config(
    "CC_REDIS_MAX_CONNECTIONS",
    cast=int,
    default=10
)