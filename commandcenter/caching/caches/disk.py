import logging
import os
import pathlib
import pickle
from typing import Any, Callable

from commandcenter.caching.caches.memo import MemoCache
from commandcenter.caching.exceptions import CacheError, CacheKeyNotFoundError



_LOGGER = logging.getLogger("commandcenter.caching.caches")


class DiskCache(MemoCache):
    """File system cache for memoized functions caches."""
    def __init__(
        self,
        key: str,
        max_entries: float,
        ttl: float,
        display_name: str,
        *,
        cache_dir: Callable[[], pathlib.Path],
    ):
        super().__init__(key, max_entries, ttl, display_name)
        self._cache_dir = cache_dir

    @property
    def cache_dir(self) -> pathlib.Path:
        return self._cache_dir()

    def read_result(self, value_key: str) -> Any:
        """Read a value and messages from the cache.
        
        Raise `CacheKeyNotFoundError` if the value doesn't exist, and `CacheError`
        if the value exists but can't be unpickled.
        """
        try:
            return super().read_result(value_key)
        except CacheKeyNotFoundError:
            pickled_entry = self._read_from_disk(value_key)
            _LOGGER.debug("Disk cache second stage HIT: %s", value_key)
            self._write_to_mem(value_key, pickled_entry)

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
        self._write_to_disk(value_key, pickled_entry)

    def invalidate(self, value_key: str) -> None:
        """Invalidate a cached value."""
        with self._mem_cache_lock:
            self._mem_cache.pop(value_key, None)
            self._remove_from_disk(value_key)

    def _read_from_disk(self, value_key: str) -> bytes:
        path = self._get_file_path(value_key)
        try:
            return path.read_bytes()
        except FileNotFoundError:
            raise CacheKeyNotFoundError("Key not found in disk cache.")
        except Exception as e:
            _LOGGER.error(
                "Unable to read from disk cache",
                exc_info=True,
                extra={"path": str(path)}
            )
            raise CacheError("Unable to read from cache.") from e

    def _write_to_disk(self, value_key: str, pickled_value: bytes) -> None:
        path = self._get_file_path(value_key)
        try:
            path.write_bytes(pickled_value)
        except Exception as e:
            _LOGGER.warning(
                "Unable to write to disk cache",
                exc_info=True,
                extra={"path": str(path)}
            )
            # Clean up file so we don't leave zero byte files.
            try:
                os.remove(path)
            except (FileNotFoundError, IOError, OSError):
                # If we can't remove the file, it's not a big deal.
                pass
            raise CacheError("Unable to write to cache") from e
    
    def _remove_from_disk(self, value_key: str) -> None:
        """Delete a cache file from disk. If the file does not exist on disk,
        return silently.
        
        If another exception occurs, log it. Does not throw.
        """
        path = self._get_file_path(value_key)
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except Exception:
            _LOGGER.warning(
                "Unable to remove a file from the disk cache",
                exc_info=True,
                extra={"path": str(path)}
            )
        else:
            _LOGGER.debug("Removed file from disk cache %s", self.key, extra={"path": str(path)})

    def _get_file_path(self, value_key: str) -> pathlib.Path:
        """Return the path of the disk cache file for the given value."""
        return self.cache_dir.joinpath(f"./{self.key}-{value_key}.memo")