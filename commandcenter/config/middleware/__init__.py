from starlette.config import Config



config = Config(".env")


CC_MIDDLEWARE_ENABLE_CONTEXT = config(
    "CC_MIDDLEWARE_ENABLE_CONTEXT",
    cast=bool,
    default=True
)
CC_MIDDLEWARE_ENABLE_LIMITING = config(
    "CC_MIDDLEWARE_ENABLE_LIMITING",
    cast=bool,
    default=True
)