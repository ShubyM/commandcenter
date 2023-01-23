import logging
import os
import pathlib
import pickle
import threading
import time
import warnings
from typing import Any, cast

from cachetools import TTLCache

from commandcenter.core.objcache.cache import AbstractCache
from commandcenter.core.objcache.exceptions import CacheError, CacheKeyNotFoundError



_LOGGER = logging.getLogger("commandcenter.caching.backends")
_TTLCACHE_TIMER = time.monotonic



class MemoryCache(AbstractCache):
    """In memory backend for memoized function caches."""
    def __init__(
        self,
        key: str,
        max_entries: float,
        ttl: float,
        display_name: str
    ):
        self.key = key
        self.display_name = display_name
        self._mem_cache: TTLCache[str, bytes] = TTLCache(
            maxsize=max_entries, ttl=ttl, timer=_TTLCACHE_TIMER
        )
        self._mem_cache_lock = threading.Lock()

    @property
    def max_entries(self) -> float:
        return cast(float, self._mem_cache.maxsize)

    @property
    def ttl(self) -> float:
        return cast(float, self._mem_cache.ttl)

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

        with self._mem_cache_lock:
            self._mem_cache[key] = pickled_entry

    def clear(self) -> None:
        """Clear all values from this function cache."""
        with self._mem_cache_lock:
            self._mem_cache.clear()

    def _read_from_mem_cache(self, key: str) -> bytes:
        with self._mem_cache_lock:
            if key in self._mem_cache:
                entry = bytes(self._mem_cache[key])
                _LOGGER.debug("Memory cache first stage HIT: %s.", key)
                return entry

            else:
                _LOGGER.debug("Memory cache MISS: %s", key)
                raise CacheKeyNotFoundError("Key not found in mem cache.")


class DiskCache(AbstractCache):
    """File system backend for memoized functions caches."""
    def __init__(
        self,
        cache_dir: os.PathLike,
        key: str,
        max_entries: float,
        ttl: float,
        display_name: str
    ):
        self.cache_dir = pathlib.Path(cache_dir)
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
        try:
            pickled_entry = self._read_from_mem_cache(key)

        except CacheKeyNotFoundError as e:
            pickled_entry = self._read_from_disk_cache(key)
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

    def clear(self) -> None:
        """Clear all values from this function cache."""
        with self._mem_cache_lock:
            # We keep a lock for the entirety of the clear operation to avoid
            # disk cache race conditions.
            for key in self._mem_cache.keys():
                self._remove_from_disk_cache(key)

            self._mem_cache.clear()

    def _read_from_mem_cache(self, key: str) -> bytes:
        with self._mem_cache_lock:
            if key in self._mem_cache:
                entry = bytes(self._mem_cache[key])
                _LOGGER.debug("Memory cache first stage HIT: %s", key)
                return entry

            else:
                _LOGGER.debug("Memory cache MISS: %s", key)
                raise CacheKeyNotFoundError("Key not found in mem cache.")

    def _read_from_disk_cache(self, key: str) -> bytes:
        path = self._get_file_path(key)
        try:
            return path.read_bytes()
        except FileNotFoundError:
            raise CacheKeyNotFoundError("Key not found in disk cache.")
        except Exception as ex:
            _LOGGER.error(ex)
            raise CacheError("Unable to read from cache.") from ex

    def _write_to_mem_cache(self, key: str, pickled_value: bytes) -> None:
        with self._mem_cache_lock:
            self._mem_cache[key] = pickled_value

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

    def _remove_from_disk_cache(self, key: str) -> None:
        """Delete a cache file from disk. If the file does not exist on disk,
        return silently.
        
        If another exception occurs, log it. Does not throw.
        """
        path = self._get_file_path(key)
        try:
            os.remove(path)
        except FileNotFoundError:
            # The file is already removed.
            pass
        except Exception as ex:
            _LOGGER.exception(
                "Unable to remove a file from the disk cache", exc_info=ex
            )

    def _get_file_path(self, value_key: str) -> pathlib.Path:
        """Return the path of the disk cache file for the given value."""
        return self.cache_dir.joinpath(f"./{self.key}-{value_key}.memo")



try:
    from redis import Redis
    from redis.exceptions import RedisError
except ImportError:
    pass


class RedisCache(AbstractCache):
    def __init__(
        self,
        redis: Redis,
        key: str,
        max_entries: float,
        ttl: float,
        display_name: str
    ) -> None:
        self.redis = redis
        self.key = key
        self.display_name = display_name
        self._mem_cache: TTLCache[str, bytes] = TTLCache(
            maxsize=max_entries, ttl=ttl, timer=_TTLCACHE_TIMER
        )
        self._mem_cache_lock = threading.Lock()


def make_cache_path(cache_dir: pathlib.Path) -> bool:
    """Create the directory(s) required to reach the cache path."""
    try:
        os.makedirs(cache_dir, exist_ok=True)
    except OSError:
        warnings.warn(
            "Failed to create the cache directory. Memoized objects cannot be "
            "persisted to disk. Objects will be stored in memory.",
            stacklevel=2
        )
        return False
    else:
        return True