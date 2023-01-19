import functools
from typing import Type

from commandcenter.config.integrations.managers import (
    CC_INTEGRATIONS_MANAGER_MAX_SUBSCRIBERS,
    CC_INTEGRATIONS_MANAGER_SUBSCRIBER_MAXLEN,
    CC_INTEGRATIONS_MANAGER
)
from commandcenter.core.integrations.abc import AbstractManager
from commandcenter.core.integrations.managers import AvailableManagers
from commandcenter.core.objcache import singleton
from commandcenter.core.util.context import source_context
from commandcenter.services.sources import build_client_for_manager



def inject_manager_dependencies(func) -> AbstractManager:
    """Wrapper around the manager setup that allows for dynamic injection 
    of the arguments based on the environment at runtime.
    """
    manager = CC_INTEGRATIONS_MANAGER
    # We make everything **kwargs instead of *args because the objcache decorator
    # can properly hash kwargs according to the argument name
    inject_kwargs = {}
    if manager is AvailableManagers.LOCAL.cls:
        inject_kwargs.update(
            {
                "max_subscribers": CC_INTEGRATIONS_MANAGER_MAX_SUBSCRIBERS,
                "maxlen": CC_INTEGRATIONS_MANAGER_SUBSCRIBER_MAXLEN,
            }
        )
    else:
        raise RuntimeError("Received invalid manager.")

    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs) -> AbstractManager:
        source = source_context.get()
        if source is None:
            raise RuntimeError(
                "Source context not set. Ensure context dependency is added to path "
                "definition."
            )
        return await func(manager, source, **inject_kwargs)

    return async_wrapper


@inject_manager_dependencies
@singleton
async def get_manager(
    manager: Type[AbstractManager],
    source: str,
    **kwargs
) -> AbstractManager:
    """Initialize manager."""
    client, subscriber, add_kwargs = build_client_for_manager(source, manager)
    kwargs.update(**add_kwargs)
    return manager(client, subscriber, **kwargs)