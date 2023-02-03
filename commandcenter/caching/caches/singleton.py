import threading
from collections.abc import Iterable
from typing import Any, Dict

from commandcenter.caching.caches.base import BaseCache
from commandcenter.caching.exceptions import CacheKeyNotFoundError


class SingletonCache(BaseCache, Iterable[Any]):
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