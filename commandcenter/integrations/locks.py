import uuid
from typing import Set

try:
    from aiomcache import Client as Memcached
    from aiomcache.exceptions import ClientException as MemcachedError
except ImportError:
    pass
try:
    from redis.asyncio import Redis
    from redis.asyncio import RedisError
except ImportError:
    pass

from commandcenter.integrations.base import BaseLock
from commandcenter.integrations.models import BaseSubscription



REGISTER_CLIENT = """
    if redis.call("GET", KEYS[1]) then
        return redis.call("PEXPIRE", KEYS[1], ARGV[1])
    else
        return redis.call("SET", KEYS[1], 1, "NX", "PX", ARGV[1])
    end
"""
RELEASE_LOCK = """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
"""
SCRIPTS = [
    REGISTER_CLIENT,
    RELEASE_LOCK,
]


class RedisLock(BaseLock):
    def __init__(self, redis: Redis, ttl: int = 5000) -> None:
        self._redis = redis
        self._ttl = ttl
        self._id = uuid.uuid4().hex

    async def acquire(self, subscriptions: Set[BaseSubscription]) -> Set[BaseSubscription]:
        return await super().acquire(subscriptions)