from starlette.config import Config

from commandcenter.core.util.cast import cast_path



config = Config(".env")


CC_COMM_PROVIDERS_TELALERT_PATH = config(
    "CC_COMM_PROVIDERS_TELALERT_PATH",
    cast=cast_path,
    default=""
)
CC_COMM_PROVIDERS_TELALERT_HOST = config(
    "CC_COMM_PROVIDERS_TELALERT_HOST",
    default=""
)
CC_COMM_PROVIDERS_TELALERT_MAX_CONCURRENCY = config(
    "CC_COMM_PROVIDERS_TELALERT_MAX_CONCURRENCY",
    cast=int,
    default=4
)
CC_COMM_PROVIDERS_TELALERT_TIMEOUT = config(
    "CC_COMM_PROVIDERS_TELALERT_TIMEOUT",
    cast=float,
    default=3
)