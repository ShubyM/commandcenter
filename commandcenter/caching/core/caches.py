import logging
import math
import os
import pathlib
import pickle
import threading
import time
from collections.abc import Iterable
from typing import Any, Callable, Dict

from cachetools import TTLCache
try:
    from redis import Redis
    from redis.exceptions import RedisError
except ImportError:
    pass

from commandcenter.caching.core.cache import Cache
from commandcenter.caching.core.exceptions import CacheError, CacheKeyNotFoundError



_LOGGER = logging.getLogger("commandcenter.caching.caches")
_TTLCACHE_TIMER = time.monotonic


class SingletonCache(Cache):
    """Manages cached values for a single singleton function."""
    def __init__(self, key: str, display_name: str):
        self.key = key
        self.display_name = display_name
        self._mem_cache: Dict[str, Any] = {}
        self._mem_cache_lock = threading.Lock()

    def read_result(self, key: str) -> Any:
        """Read a value and associated messages from the cache.

        Raise `CacheKeyNotFoundError` if the value doesn't exist.
        """
        with self._mem_cache_lock:
            try:
                return self._mem_cache[key]
            except KeyError:
                raise CacheKeyNotFoundError()
    
    def write_result(self, key: str, value: Any) -> None:
        """Write a value and associated messages to the cache."""
        with self._mem_cache_lock:
            self._mem_cache[key] = value

    def clear(self) -> None:
        """Clear all values from this function cache."""
        with self._mem_cache_lock:
            self._mem_cache.clear()

    def __iter__(self) -> Iterable[Any]:
        with self._mem_cache_lock:
            for value in self._mem_cache.values():
                yield value


class MemoCache(Cache):
    """Base in memory cache for memoized function caches."""
    def __init__(
        self,
        key: str,
        max_entries: int,
        ttl: float,
        display_name: str
    ):
        self.key = key
        self.display_name = display_name
        self._mem_cache: TTLCache[str, bytes] = TTLCache(
            maxsize=max_entries, ttl=ttl, timer=_TTLCACHE_TIMER
        )
        self._mem_cache_lock = threading.Lock()

    def read_result(self, key: str) -> Any:
        """Read a value and messages from the cache.
        
        Raise `CacheKeyNotFoundError` if the value doesn't exist, and `CacheError`
        if the value exists but can't be unpickled.
        """
        pickled_entry = self._read_from_mem_cache(key)

        try:
            return pickle.loads(pickled_entry)
        except pickle.UnpicklingError as exc:
            raise CacheError(f"Failed to unpickle {key}.") from exc

    def write_result(self, key: str, value: Any) -> None:
        """Write a value and associated messages to the cache.
        
        The value must be pickleable.
        """
        try:
            pickled_entry = pickle.dumps(value)
        except pickle.PicklingError as exc:
            raise CacheError(f"Failed to pickle {key}.") from exc

        self._write_to_mem_cache(key, pickled_entry)

    def clear(self) -> None:
        """Clear all values from this function cache."""
        self._mem_cache.clear()

    def _read_from_mem_cache(self, key: str) -> bytes:
        with self._mem_cache_lock:
            if key in self._mem_cache:
                entry = bytes(self._mem_cache[key])
                _LOGGER.debug("Cache first stage HIT: %s.", key)
                return entry

            else:
                _LOGGER.debug("Cache first stage MISS: %s", key)
                raise CacheKeyNotFoundError("Key not found in mem cache.")

    def _write_to_mem_cache(self, key: str, pickled_value: bytes) -> None:
        with self._mem_cache_lock:
            try:
                self._mem_cache[key] = pickled_value
            except ValueError:
                pass


class DiskCache(MemoCache):
    """File system cache for memoized functions caches."""
    def __init__(
        self,
        key: str,
        max_entries: float,
        ttl: float,
        display_name: str,
        *,
        cache_dir: Callable[[None], pathlib.Path],
    ):
        super().__init__(key, max_entries, ttl, display_name)
        self._cache_dir = cache_dir

    @property
    def cache_dir(self) -> pathlib.Path:
        return self._cache_dir()

    def read_result(self, key: str) -> Any:
        """Read a value and messages from the cache.
        
        Raise `CacheKeyNotFoundError` if the value doesn't exist, and `CacheError`
        if the value exists but can't be unpickled.
        """
        try:
            return super().read_result(key)
        except CacheKeyNotFoundError:
            pickled_entry = self._read_from_disk_cache(key)
            _LOGGER.debug("Disk cache second stage HIT: %s", key)
            self._write_to_mem_cache(key, pickled_entry)

        try:
            return pickle.loads(pickled_entry)
        except pickle.UnpicklingError as exc:
            raise CacheError(f"Failed to unpickle {key}.") from exc

    def write_result(self, key: str, value: Any) -> None:
        """Write a value and associated messages to the cache.
        
        The value must be pickleable.
        """
        try:
            pickled_entry = pickle.dumps(value)
        except pickle.PicklingError as exc:
            raise CacheError(f"Failed to pickle {key}.") from exc

        self._write_to_mem_cache(key, pickled_entry)
        self._write_to_disk_cache(key, pickled_entry)

    def _read_from_disk_cache(self, key: str) -> bytes:
        path = self._get_file_path(key)
        try:
            return path.read_bytes()
        except FileNotFoundError:
            raise CacheKeyNotFoundError("Key not found in disk cache.")
        except Exception as ex:
            _LOGGER.error(ex)
            raise CacheError("Unable to read from cache.") from ex

    def _write_to_disk_cache(self, key: str, pickled_value: bytes) -> None:
        path = self._get_file_path(key)
        try:
            path.write_bytes(pickled_value)
        except Exception as e:
            _LOGGER.debug(e)
            # Clean up file so we don't leave zero byte files.
            try:
                os.remove(path)
            except (FileNotFoundError, IOError, OSError):
                # If we can't remove the file, it's not a big deal.
                pass
            raise CacheError("Unable to write to cache") from e

    def _get_file_path(self, value_key: str) -> pathlib.Path:
        """Return the path of the disk cache file for the given value."""
        return self.cache_dir.joinpath(f"./{self.key}-{value_key}.memo")

    def __del__(self):
        try:
            self.clear()
        except:
            pass


class RedisCache(MemoCache):
    """redis cache for memoized functions caches."""
    def __init__(
        self,
        key: str,
        max_entries: float,
        ttl: float,
        display_name: str,
        *,
        redis: Callable[[None], "Redis"],
    ) -> None:
        super().__init__(key, max_entries, ttl, display_name)
        if ttl is math.inf:
            self._redis_ttl = None
        else:
            self._redis_ttl = ttl
        self._redis = redis
        self._enabled = True
        self._ping()

    @property
    def redis(self) -> "Redis":
        return self._redis()

    def read_result(self, key: str) -> Any:
        """Read a value and messages from the cache.
        
        Raise `CacheKeyNotFoundError` if the value doesn't exist, and `CacheError`
        if the value exists but can't be unpickled.
        """
        try:
            return super().read_result(key)
        except CacheKeyNotFoundError:
            if self._enabled:
                pickled_entry = self._read_from_redis_cache(key)
                self._write_to_mem_cache(key, pickled_entry)
            else:
                raise
        try:
            return pickle.loads(pickled_entry)
        except pickle.UnpicklingError as exc:
            raise CacheError(f"Failed to unpickle {key}.") from exc

    def write_result(self, key: str, value: Any) -> None:
        """Write a value and associated messages to the cache.
        
        The value must be pickleable.
        """
        try:
            pickled_entry = pickle.dumps(value)
        except pickle.PicklingError as exc:
            raise CacheError(f"Failed to pickle {key}.") from exc

        self._write_to_mem_cache(key, pickled_entry)
        if self._enabled:
            self._write_to_redis_cache(key, pickled_entry)

    def _ping(self) -> None:
        try:
            pong = self.redis.ping()
        except RedisError:
            _LOGGER.warning("Disabling redis cache", exc_info=True)
            self._enabled = False
        else:
            if not pong:
                _LOGGER.warning("Disabling redis cache, server did not return PONG")
                self._enabled = False

    def _read_from_redis_cache(self, key: str) -> bytes:
        # We need to use self.key in the redis key because argument hashes are
        # consistent so if two functions received the same arguments, one woould override
        # the other key
        key = self._get_redis_key(key)
        try:
            value = self.redis.get(key)
        except RedisError as e:
            _LOGGER.warning("Unable to read from redis", exc_info=True)
            self._ping()
            raise CacheError("Unable to read from cache.") from e
        except Exception as e:
            _LOGGER.error("Unhandled exception in redis get", exc_info=True)
            raise CacheError("Unable to read from cache.") from e
        if not value:
            raise CacheKeyNotFoundError("Key not found in Redis cache.")
        _LOGGER.debug("Redis cache second stage HIT: %s", key)
        return value

    def _write_to_redis_cache(self, key: str, pickled_value: bytes) -> None:
        key = self._get_redis_key(key)
        try:
            value = self.redis.set(key, pickled_value, ex=self._redis_ttl)
        except RedisError as e:
            _LOGGER.warning("Unable to write to redis", exc_info=True)
            self._ping()
            raise CacheError("Unable to write to cache.") from e
        except Exception as e:
            _LOGGER.error("Unhandled exception in redis set", exc_info=True)
            raise CacheError("Unable to write to cache.") from e
        if not value:
            raise CacheError("Unable to write to cache.")

    def _get_redis_key(self, value_key: str) -> str:
        return f"{self.key}-{value_key}"

    def __del__(self):
        try:
            self.clear()
        except:
            pass