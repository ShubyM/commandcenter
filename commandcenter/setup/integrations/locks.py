import functools
from typing import Any, Dict, Type

from commandcenter.config.integrations.locks import (
    CC_INTEGRATIONS_LOCK,
    CC_INTEGRATIONS_LOCK_TTL
)
from commandcenter.setup.memcached import configure_memcached
from commandcenter.setup.redis import configure_redis
from commandcenter.caching import singleton
from commandcenter.integrations import Lock, Locks



def inject_lock_dependencies(func) -> Lock:
    """Wrapper around the manager setup that allows for dynamic configuration."""
    lock = CC_INTEGRATIONS_LOCK
    
    hashable = {
        "ttl": CC_INTEGRATIONS_LOCK_TTL
    }
    unhashable = {}
    if lock is Locks.MEMCACHED.cls:
        from commandcenter.config.memcached import CC_MEMCACHED_MAX_CONNECTIONS
        hashable.update({"max_workers": CC_MEMCACHED_MAX_CONNECTIONS})
        unhashable.update({"memcached": configure_memcached()})
    elif lock is Locks.REDIS.cls:
        unhashable.update({"redis": configure_redis()})
    else:
        raise RuntimeError("Received invalid lock.")

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Lock:
        return func(lock, unhashable, **hashable)

    return wrapper


@inject_lock_dependencies
@singleton
def setup_lock(
    lock: Type[Lock],
    _unhashable: Dict[str, Any],
    **kwargs
) -> Lock:
    """Configure a lock from the environment.
    
    This must be run in the same thread as the event loop.
    """
    kwargs.update(_unhashable)
    return lock(**kwargs)