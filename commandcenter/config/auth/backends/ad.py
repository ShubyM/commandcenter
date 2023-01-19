from starlette.config import Config
from starlette.datastructures import CommaSeparatedStrings



config = Config(".env")


CC_AUTH_BACKENDS_AD_DOMAIN = config(
    "CC_AUTH_BACKENDS_AD_DOMAIN",
    default=""
)
CC_AUTH_BACKENDS_AD_HOSTS = config(
    "CC_AUTH_BACKENDS_AD_HOSTS",
    cast=CommaSeparatedStrings,
    default=""
)
CC_AUTH_BACKENDS_AD_TLS = config(
    "CC_AUTH_BACKENDS_AD_TLS",
    cast=bool,
    default=False
)
CC_AUTH_BACKENDS_AD_MAXCONN = config(
    "CC_AUTH_BACKENDS_AD_MAXCONN",
    cast=int,
    default=4
)