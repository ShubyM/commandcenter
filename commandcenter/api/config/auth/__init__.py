import secrets

from starlette.config import Config
from starlette.datastructures import Secret

from commandcenter.auth import AuthBackends



config = Config(".env")


CC_AUTH_SECRET_KEY = config(
    "CC_AUTH_SECRET_KEY",
    cast=Secret,
    default=secrets.token_hex(32)
)
CC_AUTH_TOKEN_EXPIRE = config(
    "CC_AUTH_TOKEN_EXPIRE",
    cast=float,
    default=1800
)
CC_AUTH_ALGORITHM = config(
    "CC_AUTH_ALGORITHM",
    default="HS256"
)
CC_AUTH_BACKEND = config(
    "CC_AUTH_BACKEND",
    cast=lambda v: AuthBackends(v).cls,
    default=AuthBackends.DEFAULT.value
)