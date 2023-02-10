import functools
from typing import Any, Dict, Type

from commandcenter.api.config.integrations.managers import (
    CC_INTEGRATIONS_MANAGER,
    CC_INTEGRATIONS_MANAGER_INITIAL_BACKOFF,
    CC_INTEGRATIONS_MANAGER_MAX_BACKOFF,
    CC_INTEGRATIONS_MANAGER_MAX_FAILED_ATTEMPTS,
    CC_INTEGRATIONS_MANAGER_MAX_SUBSCRIBERS,
    CC_INTEGRATIONS_MANAGER_SUBSCRIBER_MAXLEN,
    CC_INTEGRATIONS_MANAGER_TIMEOUT
)
from commandcenter.api.setup.integrations.locks import setup_lock
from commandcenter.api.setup.rabbitmq import setup_rabbitmq
from commandcenter.api.setup.redis import setup_redis
from commandcenter.api.setup.sources import setup_client
from commandcenter.caching import singleton
from commandcenter.context import source_context
from commandcenter.integrations import Manager, Managers



def inject_manager_dependencies(func) -> Manager:
    """Wrapper around the manager setup that allows for dynamic configuration."""
    manager = CC_INTEGRATIONS_MANAGER
    
    hashable = {
        "max_subscribers": CC_INTEGRATIONS_MANAGER_MAX_SUBSCRIBERS,
        "maxlen": CC_INTEGRATIONS_MANAGER_SUBSCRIBER_MAXLEN,
    }
    unhashable = {}
    requires_lock = False
    if manager is Managers.LOCAL.cls:
        pass
    elif manager is Managers.RABBITMQ.cls:
        unhashable.update({"factory": setup_rabbitmq()})
        hashable.update(
            {
                "timeout": CC_INTEGRATIONS_MANAGER_TIMEOUT,
                "max_backoff": CC_INTEGRATIONS_MANAGER_MAX_BACKOFF,
                "initial_backoff": CC_INTEGRATIONS_MANAGER_INITIAL_BACKOFF,
                "max_failed": CC_INTEGRATIONS_MANAGER_MAX_FAILED_ATTEMPTS
            }
        )
        requires_lock = True
    elif manager is Managers.REDIS.cls:
        unhashable.update({"redis": setup_redis()})
        hashable.update(
            {
                "timeout": CC_INTEGRATIONS_MANAGER_TIMEOUT,
                "max_backoff": CC_INTEGRATIONS_MANAGER_MAX_BACKOFF,
                "initial_backoff": CC_INTEGRATIONS_MANAGER_INITIAL_BACKOFF,
                "max_failed": CC_INTEGRATIONS_MANAGER_MAX_FAILED_ATTEMPTS
            }
        )
        requires_lock = True
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
        return func(
            manager,
            source,
            requires_lock,
            unhashable,
            **hashable
        )

    return wrapper


@inject_manager_dependencies
@singleton
def setup_manager(
    manager: Type[Manager],
    source: str,
    requires_lock: bool,
    _unhashable: Dict[str, Any],
    **kwargs
) -> Manager:
    """Configure a manager from the environment.
    
    This must be run in the same thread as the event loop.
    """
    kwargs.update(_unhashable)
    client, subscriber, add_kwargs = setup_client(source, manager)
    kwargs.update(add_kwargs)
    if requires_lock:
        lock = setup_lock()
        return manager(client, subscriber, lock=lock, **kwargs)
    else:
        return manager(client, subscriber, **kwargs)