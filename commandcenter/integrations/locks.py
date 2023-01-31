import asyncio
import logging
import re
import uuid
from typing import List, Set

import anyio
try:
    from pymemcache import PooledClient as Memcached
    from pymemcache.exceptions import MemcacheError
except ImportError:
    pass
try:
    from redis.asyncio import Redis
    from redis.asyncio import RedisError
except ImportError:
    pass

from commandcenter.caching import memo
from commandcenter.integrations.base import BaseLock
from commandcenter.integrations.exceptions import LockingError
from commandcenter.integrations.models import BaseSubscription
from commandcenter.util import ObjSelection



_LOGGER = logging.getLogger("commandcenter.integrations.locks")

RELEASE_LOCK = """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
"""
LUA_SCRIPTS = {"release": RELEASE_LOCK}


@memo
async def register_lua_script(script: str, _redis: Redis) -> str:
    script = re.sub(r'^\s+', '', script, flags=re.M).strip()
    return await _redis.script_load(script)


async def redis_extend(redis: Redis, hashes: List[str], ttl: int) -> None:
    """Runs a PEXPIRE command on a sequence of keys."""
    try:
        async with redis.pipeline(transaction=True) as pipe:
            for hash_ in hashes:
                pipe.pexpire(hash_, ttl)
            await pipe.execute()
    except RedisError:
        _LOGGER.warning("Error in redis client", exc_info=True)


async def redis_poll(
    redis: Redis,
    subscriptions: List[BaseSubscription],
    hashes: List[str]
) -> List[BaseSubscription]:
    """Runs a GET command on a sequence of keys. Returns the subscriptions which
    dont exist.
    """
    try:
        async with redis.pipeline(transaction=True) as pipe:
            for hash_ in hashes:
                pipe.get(hash_)
            results = await pipe.execute()
        return set([subscription for subscription, result in zip(subscriptions, results) if not result])
    except RedisError:
        _LOGGER.warning("Error in redis client", exc_info=True)
        return set()


def memcached_poll(
    memcached: Memcached,
    subscriptions: List[BaseSubscription],
    hashes: List[str]
) -> List[BaseSubscription]:
    """Runs a GET command on a sequence of keys. Returns the subscriptions which
    exist.
    """
    try:
        results = [memcached.get(hash_) for hash_ in hashes]
        return set([subscription for subscription, result in zip(subscriptions, results) if not result])
    except MemcacheError:
        _LOGGER.warning("Error in memcached client", exc_info=True)
        #return set()


def memcached_release(
    memcached: Memcached,
    id_: bytes,
    hashes: List[str]
) -> None:
    try:
        results = [memcached.get(hash_) for hash_ in hashes]
        hashes = [hash_ for hash_, result in zip(hashes, results) if result == id_]
        if hashes:
            memcached.delete_many(hashes)
    except MemcacheError:
        _LOGGER.warning("Error in memcached client", exc_info=True)


def memcached_extend(
    memcached: Memcached,
    id_: bytes,
    hashes: List[str],
    ttl: int
) -> List[str]:
    try:
        results = [memcached.get(hash_) for hash_ in hashes]
        hashes = [hash_ for hash_, result in zip(hashes, results) if result == id_]
        if hashes:
            values = {hash_: id_ for hash_ in hashes}
            memcached.set_many(values, ttl)
            _LOGGER.debug("Extended %i client subscriptions", len(hashes))
    except MemcacheError:
        _LOGGER.warning("Error in memcached client", exc_info=True)

class RedisLock(BaseLock):
    """Lock implementation with Redis backend.
    
    Args:
        redis: The redis client.
        ttl: The time in milliseconds to acquire and extend locks for.
    """
    def __init__(self, redis: "Redis", ttl: int = 5000) -> None:
        self._redis = redis
        self._ttl = ttl
        self._id = uuid.uuid4().hex

    @property
    def ttl(self) -> float:
        return self._ttl/1000

    async def acquire(self, subscriptions: Set[BaseSubscription]) -> Set[BaseSubscription]:
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                id_ = self._id
                ttl = self._ttl
                for hash_ in hashes:
                    pipe.set(hash_, id_, px=ttl, nx=True)
                results = await pipe.execute()
        except RedisError as e:
            _LOGGER.warning("Error in redis client", exc_info=True)
            raise LockingError() from e
        return set([subscription for subscription, result in zip(subscriptions, results) if result])

    async def register(self, subscriptions: Set[BaseSubscription]) -> None:
        subscriptions = sorted(subscriptions)
        hashes = [self.subscriber_key(subscription) for subscription in subscriptions]
        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                id_ = self._id
                ttl = self._ttl
                for hash_ in hashes:
                    pipe.set(hash_, id_, px=ttl)
                await pipe.execute()
        except RedisError:
            _LOGGER.warning("Error in redis client", exc_info=True)

    async def release(self, subscriptions: Set[BaseSubscription]) -> None:
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        script = LUA_SCRIPTS["release"]
        try:
            sha = await register_lua_script(script, self._redis)
            async with self._redis.pipeline(transaction=True) as pipe:
                id_ = self._id
                for hash_ in hashes:
                    args = (hash_, id_)
                    pipe.evalsha(sha, 1, *args)
                await pipe.execute()
        except RedisError:
            _LOGGER.warning("Error in redis client", exc_info=True)

    async def extend_client(self, subscriptions: Set[BaseSubscription]) -> None:
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        await redis_extend(self._redis, hashes, self._ttl)

    async def extend_subscriber(self, subscriptions: Set[BaseSubscription]) -> None:
        subscriptions = sorted(subscriptions)
        hashes = [self.subscriber_key(subscription) for subscription in subscriptions]
        await redis_extend(self._redis, hashes, self._ttl)

    async def client_poll(self, subscriptions: Set[BaseSubscription]) -> Set[BaseSubscription]:
        subscriptions = sorted(subscriptions)
        hashes = [self.subscriber_key(subscription) for subscription in subscriptions]
        return await redis_poll(self._redis, subscriptions, hashes)

    async def subscriber_poll(self, subscriptions: Set[BaseSubscription]) -> Set[BaseSubscription]:
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        return await redis_poll(self._redis, subscriptions, hashes)


class MemcachedLock(BaseLock):
    """Lock implementation with Memcached backend.
    
    Args:
        memcached: The memcached client.
        ttl: The time in milliseconds to acquire and extend locks for.
    """
    def __init__(self, memcached: "Memcached", ttl: int = 5000, max_workers: int = 4) -> None:
        self._memcached = memcached
        self._ttl = int(ttl/1000)
        self._id = uuid.uuid4().hex
        self._limiter = anyio.CapacityLimiter(max_workers)

    @property
    def ttl(self) -> float:
        return self._ttl

    async def acquire(self, subscriptions: Set[BaseSubscription]) -> Set[BaseSubscription]:
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        try:
            id_ = self._id
            ttl = self._ttl
            dispatch = [
                anyio.to_thread.run_sync(
                    self._memcached.add,
                    hash_,
                    id_,
                    ttl,
                    limiter=self._limiter
                )
                for hash_ in hashes
            ]
            results = await asyncio.gather(*dispatch)
        except MemcacheError as e:
            _LOGGER.warning("Error in memcached client", exc_info=True)
            raise LockingError() from e
        return set([subscription for subscription, stored in zip(subscriptions, results) if stored])

    async def register(self, subscriptions: Set[BaseSubscription]) -> None:
        subscriptions = sorted(subscriptions)
        hashes = [self.subscriber_key(subscription) for subscription in subscriptions]
        try:
            id_ = self._id
            ttl = self._ttl
            values = {hash_: id_ for hash_ in hashes}
            await anyio.to_thread.run_sync(
                self._memcached.set_many,
                values,
                ttl,
                limiter=self._limiter
            )
            _LOGGER.debug("Registered %i subscriptions", len(hashes))
        except MemcacheError:
            _LOGGER.warning("Error in memcached client", exc_info=True)

    async def release(self, subscriptions: Set[BaseSubscription]) -> None:
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        id_ = self._id.encode()
        await anyio.to_thread.run_sync(
            memcached_release,
            self._memcached,
            id_,
            hashes,
            limiter=self._limiter
        )

    async def extend_client(self, subscriptions: Set[BaseSubscription]) -> None:
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        id_ = self._id.encode()
        ttl = self._ttl
        await anyio.to_thread.run_sync(
            memcached_extend,
            self._memcached,
            id_,
            hashes,
            ttl,
            limiter=self._limiter
        )

    async def extend_subscriber(self, subscriptions: Set[BaseSubscription]) -> None:
        await self.register(subscriptions)
    
    async def client_poll(self, subscriptions: Set[BaseSubscription]) -> Set[BaseSubscription]:
        subscriptions = sorted(subscriptions)
        hashes = [self.subscriber_key(subscription) for subscription in subscriptions]
        return await anyio.to_thread.run_sync(
            memcached_poll,
            self._memcached,
            subscriptions,
            hashes,
            limiter=self._limiter
        )

    async def subscriber_poll(self, subscriptions: Set[BaseSubscription]) -> Set[BaseSubscription]:
        subscriptions = sorted(subscriptions)
        hashes = [str(hash(subscription)) for subscription in subscriptions]
        return await anyio.to_thread.run_sync(
            memcached_poll,
            self._memcached,
            subscriptions,
            hashes,
            limiter=self._limiter
        )


class Locks(ObjSelection):
    DEFAULT = "default", MemcachedLock
    MEMCACHED = "memcached", MemcachedLock
    REDIS = "redis", RedisLock