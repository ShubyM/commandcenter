from commandcenter.util import ObjSelection
from .memcached import MemcachedLock
from .redis import RedisLock



__all__ = [
    "MemcachedLock",
    "RedisLock",
    "Locks"
]


class Locks(ObjSelection):
    DEFAULT = "default", RedisLock
    MEMCACHED = "memcached", MemcachedLock
    REDIS = "redis", RedisLock