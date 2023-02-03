import logging
import pickle
import threading
import time
from typing import Any

from cachetools import TTLCache

from commandcenter.caching.caches.base import BaseCache
from commandcenter.caching.exceptions import CacheError, CacheKeyNotFoundError



_LOGGER = logging.getLogger("commandcenter.caching.caches")
_TTLCACHE_TIMER = time.monotonic


class MemoCache(BaseCache):
    """In memory cache for a single memoized function."""
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

    def read_result(self, value_key: str) -> Any:
        """Read a value and messages from the cache.
        
        Raise `CacheKeyNotFoundError` if the value doesn't exist, and `CacheError`
        if the value exists but can't be unpickled.
        """
        pickled_entry = self._read_from_mem(value_key)

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

    def clear(self) -> None:
        """Clear all values from this function cache."""
        self._mem_cache.clear()

    def invalidate(self, value_key: str) -> None:
        """Invalidate a cached value."""
        with self._mem_cache_lock:
            self._mem_cache.pop(value_key, None)

    def _read_from_mem(self, value_key: str) -> bytes:
        with self._mem_cache_lock:
            if value_key in self._mem_cache:
                entry = bytes(self._mem_cache[value_key])
                _LOGGER.debug("Cache first stage HIT: %s.", value_key)
                return entry

            else:
                _LOGGER.debug("Cache first stage MISS: %s", value_key)
                raise CacheKeyNotFoundError("Key not found in mem cache.")

    def _write_to_mem(self, value_key: str, pickled_value: bytes) -> None:
        with self._mem_cache_lock:
            try:
                self._mem_cache[value_key] = pickled_value
            except ValueError:
                pass