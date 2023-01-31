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
CC_INTEGRATIONS_MANAGER_TIMEOUT = config(
    "CC_INTEGRATION_MANAGER_TIMEOUT",
    cast=float,
    default=5
)
CC_INTEGRATIONS_MANAGER_MAX_BACKOFF = config(
    "CC_INTEGRATIONS_MANAGER_MAX_BACKOFF",
    cast=float,
    default=5
)
CC_INTEGRATIONS_MANAGER_INITIAL_BACKOFF = config(
    "CC_INTEGRATIONS_MANAGER_INITIAL_BACKOFF",
    cast=float,
    default=1
)
CC_INTEGRATIONS_MANAGER_MAX_FAILED_ATTEMPTS = config(
    "CC_INTEGRATIONS_MANAGER_MAX_FAILED_ATTEMPTS",
    cast=int,
    default=15
)