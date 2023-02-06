from starlette.config import Config

from commandcenter.__version__ import __title__ as TITLE


config = Config(".env")


CC_MONGO_URL = config(
    "CC_MONGODB_URL",
    default="mongodb://localhost:27017/"
)
CC_MONGO_HEARTBEAT = config(
    "CC_MONGODB_HEARTBEAT",
    cast=int,
    default=10000
)
CC_MONGO_SERVER_SELECTION_TIMEOUT = config(
    "CC_MONGODB_SERVER_SELECTION_TIMEOUT",
    cast=int,
    default=10000
)
CC_MONGO_CONNECT_TIMEOUT = config(
    "CC_MONGODB_CONNECT_TIMEOUT",
    cast=int,
    default=5000
)
CC_MONGO_TIMEOUT = config(
    "CC_MONGO_TIMEOUT",
    cast=int,
    default=10000
)
CC_MONGO_MAX_POOL_SIZE = config(
    "CC_MONGO_MAX_POOL_SIZE",
    cast=int,
    default=10
)
CC_MONGO_APPNAME = config(
    "CC_MONGO_APPNAME",
    default=TITLE
)