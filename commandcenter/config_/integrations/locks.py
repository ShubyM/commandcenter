from starlette.config import Config

from commandcenter.core.integrations.locks import AvailableLocks



config = Config(".env")


CC_INTEGRATIONS_LOCK = config(
    "CC_INTEGRATIONS_LOCK",
    cast=lambda v: AvailableLocks(v).cls,
    default=AvailableLocks.DEFAULT.value
)
CC_INTEGRATIONS_LOCK_TTL = config(
    "CC_INTEGRATIONS_LOCK_TTL",
    cast=int,
    default=5000
)