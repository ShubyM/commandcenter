import logging
import pickle
from typing import Any, Callable

try:
    from redis import Redis
except ImportError:
    pass

from commandcenter.caching.caches.memo import MemoCache
from commandcenter.caching.exceptions import CacheError, CacheKeyNotFoundError



_LOGGER = logging.getLogger("commandcenter.caching.caches")


class RedisCache(MemoCache):
    """Redis cache for memoized functions caches."""
    def __init__(
        self,
        key: str,
        max_entries: float,
        ttl: float,
        display_name: str,
        *,
        redis: Callable[[], "Redis"],
    ) -> None:
        super().__init__(key, max_entries, ttl, display_name)
        self._redis_ttl = ttl
        self._redis = redis
        self._enabled = True
        self._ping()

    @property
    def redis(self) -> "Redis":
        return self._redis()

    def read_result(self, value_key: str) -> Any:
        """Read a value and messages from the cache.
        
        Raise `CacheKeyNotFoundError` if the value doesn't exist, and `CacheError`
        if the value exists but can't be unpickled.
        """
        try:
            return super().read_result(value_key)
        except CacheKeyNotFoundError:
            if self._enabled:
                pickled_entry = self._read_from_redis(value_key)
                self._write_to_mem(value_key, pickled_entry)
            else:
                raise
        try:
            return pickle.loads(pickled_entry)
        except pickle.UnpicklingError as e:
            raise CacheError(f"Failed to unpickle {value_key}.") from e

    def write_result(self, value_key: str, value: Any) -> None:
        """Write a value and associated messages to the cache.
        
        The value must be pickleable.
        """
        try:
            pickled_entry = pickle.dumps(value)
        except pickle.PicklingError as e:
            raise CacheError(f"Failed to pickle {value_key}.") from e

        self._write_to_mem(value_key, pickled_entry)
        if self._enabled:
            self._write_to_redis(value_key, pickled_entry)
    
    def invalidate(self, value_key: str) -> None:
        """Invalidate a cached value."""
        super().invalidate(value_key)
        self._remove_from_redis(value_key)

    def _ping(self) -> None:
        try:
            pong = self.redis.ping()
        except Exception:
            _LOGGER.error("Disabling redis cache", exc_info=True)
            self._enabled = False
        else:
            if not pong:
                _LOGGER.error("Disabling redis cache, server did not return PONG")
                self._enabled = False
        if not self._enabled:
            self.redis.close()

    def _read_from_redis(self, value_key: str) -> bytes:
        key = self._get_redis_key(value_key)
        try:
            value = self.redis.get(key)
        except Exception as e:
            _LOGGER.warning("Unable to read from redis cache", exc_info=True)
            self._ping()
            raise CacheError("Unable to read from cache.") from e
        if value is None:
            raise CacheKeyNotFoundError("Key not found in Redis cache.")
        _LOGGER.debug("Redis cache second stage HIT: %s", key)
        return value

    def _write_to_redis(self, value_key: str, pickled_value: bytes) -> None:
        key = self._get_redis_key(value_key)
        try:
            ok = self.redis.set(key, pickled_value, ex=self._redis_ttl)
        except Exception as e:
            _LOGGER.warning("Unable to write to redis cache", exc_info=True)
            self._ping()
            raise CacheError("Unable to write to cache.") from e
        if not ok:
            raise CacheError("Unable to write to cache.")

    def _remove_from_redis(self, value_key: str) -> None:
        """Delete a cache key from Redis. If the cache key does not exist,
        return silently.
        
        If another exception occurs, log it. Does not throw.
        """
        key = self._get_redis_key(value_key)
        try:
            deleted = self.redis.delete(key)
        except Exception:
            _LOGGER.warning("Unable to delete from redis cache", exc_info=True)
            self._ping()
        _LOGGER.debug("Deleted %i keys from redis cache matching %s", deleted, key)

    def _get_redis_key(self, value_key: str) -> str:
        return f"{self.key}-{value_key}"