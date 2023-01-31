from commandcenter.caching.memo import caches, memo
from commandcenter.config.caching import CC_CACHE_DIR, CC_CACHE_BACKEND
from commandcenter.setup.memcached import setup_memcached
from commandcenter.setup.redis import setup_redis



def setup_caching():
    """Set the caching strategy for memoized functions."""
    if CC_CACHE_DIR:
        memo.set_cache_dir(CC_CACHE_DIR)
    if CC_CACHE_BACKEND is caches.MEMCACHED.cls:
        memcached = setup_memcached()
        memo.set_memcached_client(memcached)
        memo.set_default_backend(caches.MEMCACHED.value)
    elif CC_CACHE_BACKEND is caches.REDIS.cls:
        redis = setup_redis()
        memo.set_redis_client(redis)
        memo.set_default_backend(caches.REDIS.value)
    