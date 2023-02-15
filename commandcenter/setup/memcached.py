from commandcenter.config.memcached import (
    CC_MEMCACHED_CONNECT_TIMEOUT,
    CC_MEMCACHED_MAX_CONNECTIONS,
    CC_MEMCACHED_TIMEOUT,
    CC_MEMCACHED_URL
)



def configure_memcached():
    """Configure a Memcached client from the environment."""
    try:
        from pymemcache.client import PooledClient
    except ImportError:
        raise RuntimeError(
            "Attempted to use memcached support, but the `pymemcache` package is not "
            "installed. Use 'pip install commandcenter[memcached]'."
        )
    return PooledClient(
        CC_MEMCACHED_URL,
        connect_timeout=CC_MEMCACHED_CONNECT_TIMEOUT,
        timeout=CC_MEMCACHED_TIMEOUT,
        max_pool_size=CC_MEMCACHED_MAX_CONNECTIONS,
        default_noreply=False
    )