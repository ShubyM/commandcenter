from commandcenter.api.config.caching import CC_CACHE_DIR, CC_CACHE_BACKEND
from commandcenter.api.setup.memcached import setup_memcached
from commandcenter.api.setup.redis import setup_redis
from commandcenter.caching import Caches, memo




def setup_caching():
    """Set the caching strategy for memoized functions."""
    if CC_CACHE_DIR:
        memo.set_cache_dir(CC_CACHE_DIR)
    if CC_CACHE_BACKEND is Caches.MEMCACHED.cls:
        memcached = setup_memcached()
        memo.set_memcached_client(memcached)
        memo.set_default_backend(Caches.MEMCACHED.value)
    elif CC_CACHE_BACKEND is Caches.REDIS.cls:
        redis = setup_redis(sync=True)
        memo.set_redis_client(redis)
        memo.set_default_backend(Caches.REDIS.value)
    