from starlette.config import Config

from commandcenter.core.integrations.managers import AvailableManagers



config = Config(".env")


CC_INTEGRATIONS_MANAGER = config(
    "CC_INTEGRATIONS_MANAGER",
    cast=lambda v: AvailableManagers(v).cls,
    default=AvailableManagers.DEFAULT.value
)
CC_INTEGRATIONS_MANAGER_MAX_SUBSCRIBERS = config(
    "CC_INTEGRATIONS_MANAGER_MAX_SUBSCRIBERS",
    cast=int,
    default=100
)
CC_INTEGRATIONS_MANAGER_SUBSCRIBER_MAXLEN = config(
    "CC_INTEGRATIONS_MANAGER_SUBSCRIBER_MAXLEN",
    cast=int,
    default=100
)