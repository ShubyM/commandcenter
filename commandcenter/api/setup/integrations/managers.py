import functools
from typing import Any, Callable, Dict, Type

from commandcenter.config.integrations.managers import (
    CC_INTEGRATIONS_MANAGER,
    CC_INTEGRATIONS_MANAGER_INITIAL_BACKOFF,
    CC_INTEGRATIONS_MANAGER_MAX_BACKOFF,
    CC_INTEGRATIONS_MANAGER_MAX_FAILED_ATTEMPTS,
    CC_INTEGRATIONS_MANAGER_MAX_SUBSCRIBERS,
    CC_INTEGRATIONS_MANAGER_SUBSCRIBER_MAXLEN,
    CC_INTEGRATIONS_MANAGER_TIMEOUT
)
from commandcenter.caching import singleton
from commandcenter.context import source_context
from commandcenter.integrations.managers import Managers
from commandcenter.integrations.protocols import Manager
from commandcenter.setup.integrations.locks import setup_lock
from commandcenter.setup.rabbitmq import setup_rabbitmq
from commandcenter.setup.redis import setup_redis
from commandcenter.setup.sources import setup_client



def inject_manager_dependencies(func) -> Manager:
    """Wrapper around the manager setup that allows for dynamic configuration."""
    manager = CC_INTEGRATIONS_MANAGER
    
    inject_kwargs = {
        "max_subscribers": CC_INTEGRATIONS_MANAGER_MAX_SUBSCRIBERS,
        "maxlen": CC_INTEGRATIONS_MANAGER_SUBSCRIBER_MAXLEN,
    }
    callables = {}
    requires_lock = False
    if manager is Managers.LOCAL.cls:
        pass
    elif manager is Managers.RABBITMQ.cls:
        callables.update({"factory": functools.partial(setup_rabbitmq)})
        inject_kwargs.update(
            {
                "timeout": CC_INTEGRATIONS_MANAGER_TIMEOUT,
                "max_backoff": CC_INTEGRATIONS_MANAGER_MAX_BACKOFF,
                "initial_backoff": CC_INTEGRATIONS_MANAGER_INITIAL_BACKOFF,
                "max_failed": CC_INTEGRATIONS_MANAGER_MAX_FAILED_ATTEMPTS
            }
        )
        requires_lock = True
    elif manager is Managers.REDIS.cls:
        callables.update({"redis": functools.partial(setup_redis)})
        inject_kwargs.update(
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
            callables,
            requires_lock,
            **inject_kwargs
        )

    return wrapper


@inject_manager_dependencies
@singleton
def setup_manager(
    manager: Type[Manager],
    source: str,
    _callables: Dict[str, Callable[[], Any]],
    requires_lock: bool,
    **kwargs
) -> Manager:
    """Initialize manager from the runtime configuration.
    
    This must be run in the same thread as the event loop.
    """
    kwargs.update({k: v() for k, v in _callables.items()})
    client, subscriber, add_kwargs = setup_client(source, manager)
    kwargs.update(add_kwargs)
    if requires_lock:
        lock = setup_lock()
        return manager(client, subscriber, lock=lock, **kwargs)
    else:
        return manager(client, subscriber, **kwargs)