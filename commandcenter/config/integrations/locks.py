from starlette.config import Config

from commandcenter.integrations.locks import Locks



config = Config(".env")


CC_INTEGRATIONS_LOCK = config(
    "CC_INTEGRATIONS_LOCK",
    cast=lambda v: Locks(v).cls,
    default=Locks.DEFAULT.value
)
CC_INTEGRATIONS_LOCK_TTL = config(
    "CC_INTEGRATIONS_LOCK_TTL",
    cast=int,
    default=5000
)