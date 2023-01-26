from starlette.config import Config

from commandcenter.integrations.managers import Managers



config = Config(".env")


CC_INTEGRATIONS_MANAGER = config(
    "CC_INTEGRATIONS_MANAGER",
    cast=lambda v: Managers(v).cls,
    default=Managers.DEFAULT.value
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