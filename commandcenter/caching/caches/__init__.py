from commandcenter.util import ObjSelection
from .base import BaseCache
from .disk import DiskCache
from .memcached import MemcachedCache
from .memo import MemoCache
from .redis import RedisCache
from .singleton import SingletonCache



__all__ = [
    "BaseCache",
    "DiskCache",
    "MemcachedCache",
    "MemoCache",
    "RedisCache",
    "SingletonCache",
    "Caches",
]


class Caches(ObjSelection):
    SINGLETON = "singleton", SingletonCache
    MEMORY = "memory", MemoCache
    DISK = "disk", DiskCache
    REDIS = "redis", RedisCache
    MEMCACHED = "memcached", MemcachedCache