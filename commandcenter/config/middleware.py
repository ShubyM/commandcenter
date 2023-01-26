from starlette.config import Config
from starlette.datastructures import CommaSeparatedStrings



config = Config(".env")


CC_MIDDLEWARE_ENABLE_CONTEXT = config(
    "CC_MIDDLEWARE_ENABLE_CONTEXT",
    cast=bool,
    default=True
)
CC_MIDDLEWARE_ENABLE_CORS = config(
    "CC_MIDDLEWARE_ENABLE_CORS",
    cast=bool,
    default=True
)
CC_MIDDLEWARE_CORS_ORIGINS = config(
    "CC_MIDDLEWARE_CORS_ORIGINS",
    cast=CommaSeparatedStrings,
    default="http://localhost*,https://localhost*"
)