import functools
from typing import Type

from commandcenter.config.integrations.managers import (
    CC_INTEGRATIONS_MANAGER_MAX_SUBSCRIBERS,
    CC_INTEGRATIONS_MANAGER_SUBSCRIBER_MAXLEN,
    CC_INTEGRATIONS_MANAGER
)
from commandcenter.caching import singleton
from commandcenter.context import source_context
from commandcenter.integrations.managers import Managers
from commandcenter.integrations.protocols import Manager
from commandcenter.setup.sources import setup_client



def inject_manager_dependencies(func) -> Manager:
    """Wrapper around the manager setup that allows for dynamic configuration."""
    manager = CC_INTEGRATIONS_MANAGER
    
    inject_kwargs = {}
    if manager is Managers.LOCAL.cls:
        inject_kwargs.update(
            {
                "max_subscribers": CC_INTEGRATIONS_MANAGER_MAX_SUBSCRIBERS,
                "maxlen": CC_INTEGRATIONS_MANAGER_SUBSCRIBER_MAXLEN,
            }
        )
    else:
        raise RuntimeError("Received invalid manager.")

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Manager:
        source = source_context.get() # Requires dependency
        if source is None:
            raise RuntimeError(
                "Source context not set. Ensure context dependency is added to path "
                "definition."
            )
        return func(manager, source, **inject_kwargs)

    return wrapper


@inject_manager_dependencies
@singleton
def setup_manager(
    manager: Type[Manager],
    source: str,
    **kwargs
) -> Manager:
    """Initialize manager from the runtime configuration.
    
    This must be run in the same thread as the event loop.
    """
    client, subscriber, add_kwargs = setup_client(source, manager)
    kwargs.update(**add_kwargs)
    return manager(client, subscriber, **kwargs)