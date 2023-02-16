from commandcenter.config.caching import CC_CACHE_DIR, CC_CACHE_BACKEND
from commandcenter.setup.memcached import configure_memcached
from commandcenter.setup.redis import configure_redis
from commandcenter.caching import (
    Caches,
    set_cache_dir,
    set_default_backend,
    set_memcached_client,
    set_redis_client
)




def setup_caching():
    """Set the caching strategy for memoized functions."""
    if CC_CACHE_DIR:
        set_cache_dir(CC_CACHE_DIR)
    if CC_CACHE_BACKEND is Caches.MEMCACHED.cls:
        memcached = configure_memcached()
        set_memcached_client(memcached)
        set_default_backend(Caches.MEMCACHED.value)
    elif CC_CACHE_BACKEND is Caches.REDIS.cls:
        redis = configure_redis(sync=True)
        set_redis_client(redis)
        set_default_backend(Caches.REDIS.value)
    