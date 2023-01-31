import functools
from typing import Any, Callable, Dict, Type

from commandcenter.config.integrations.locks import (
    CC_INTEGRATIONS_LOCK,
    CC_INTEGRATIONS_LOCK_TTL
)
from commandcenter.caching import singleton
from commandcenter.integrations.locks import Locks
from commandcenter.integrations.protocols import Lock
from commandcenter.setup.memcached import setup_memcached
from commandcenter.setup.redis import setup_redis



def inject_lock_dependencies(func) -> Lock:
    """Wrapper around the manager setup that allows for dynamic configuration."""
    lock = CC_INTEGRATIONS_LOCK
    
    inject_kwargs = {
        "ttl": CC_INTEGRATIONS_LOCK_TTL
    }
    callables = {} # Use partials which are hashable in @singleton
    if lock is Locks.MEMCACHED.cls:
        from commandcenter.config.memcached import CC_MEMCACHED_MAX_CONNECTIONS
        inject_kwargs.update({"max_workers": CC_MEMCACHED_MAX_CONNECTIONS})
        callables.update({"memcached": functools.partial(setup_memcached)})
    elif lock is Locks.REDIS.cls:
        callables.update({"redis": functools.partial(setup_redis)})
    else:
        raise RuntimeError("Received invalid lock.")

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Lock:
        return func(lock, callables, **inject_kwargs)

    return wrapper


@inject_lock_dependencies
@singleton
def setup_lock(
    lock: Type[Lock],
    _callables: Dict[str, Callable[[], Any]],
    **kwargs
) -> Lock:
    """Initialize lock from the runtime configuration."""
    kwargs.update({k: v() for k, v in _callables.items()})
    return lock(**kwargs)