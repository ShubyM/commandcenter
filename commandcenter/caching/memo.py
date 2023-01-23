import asyncio
import functools
import inspect
import logging
import math
import os
import pathlib
import pickle
import shutil
import threading
import time
import types
import warnings
from datetime import timedelta
from typing import Any, Callable, Dict, Optional, Type, TypeVar, Union

from commandcenter.caching.core.backends import (
    DiskCache,
    MemoryCache
)
from commandcenter.caching.core.cache import (
    AbstractCache,
    CachedFunction,
    create_cache_wrapper
)
from commandcenter.caching.core.util import CacheType



_LOGGER = logging.getLogger("commandcenter.caching.memo")
_BACKEND: Type[AbstractCache] = MemoryCache


class MemoizedFunction(CachedFunction):
    """Implements the `CachedFunction` protocol for `@memo`"""
    def __init__(
        self,
        func: types.FunctionType,
        persist: bool,
        max_entries: Optional[int],
        ttl: Optional[float]
    ):
        super().__init__(func)
        self.persist = persist
        self.max_entries = max_entries
        self.ttl = ttl

    @property
    def cache_type(self) -> CacheType:
        return CacheType.MEMO

    @property
    def display_name(self) -> str:
        """A human-readable name for the cached function"""
        return f"{self.func.__module__}.{self.func.__qualname__}"

    def get_function_cache(self, function_key: str) -> AbstractCache:
        return _memo_caches.get_cache(
            key=function_key,
            persist=self.persist,
            max_entries=self.max_entries,
            ttl=self.ttl,
            display_name=self.display_name
        )


class MemoCaches:
    """Manages all MemoCache instances"""
    def __init__(self):
        self._caches_lock = threading.Lock()
        self._function_caches: Dict[str, AbstractCache] = {}

    def get_cache(
        self,
        key: str,
        persist: Optional[str],
        max_entries: Optional[int | float],
        ttl: Optional[int | float],
        display_name: str
    ) -> AbstractCache:
        """Return the cache for the given key.

        If it doesn't exist, create a new one with the given params.
        """
        if max_entries is None:
            max_entries = math.inf
        if ttl is None:
            ttl = math.inf

        # Get the existing cache, if it exists, and validate that its params
        # haven't changed.
        with self._caches_lock:
            cache = self._function_caches.get(key)
            if (
                cache is not None
                and cache.ttl == ttl
                and cache.max_entries == max_entries
                and cache.persist == persist
            ):
                return cache

            # Create a new cache object and put it in our dict
            _LOGGER.debug(
                "Creating new MemoCache (key=%s, persist=%s, max_entries=%s, ttl=%s)",
                key,
                persist,
                max_entries,
                ttl,
            )
            cache = MemoCache(
                key=key,
                persist=persist,
                max_entries=max_entries,
                ttl=ttl,
                display_name=display_name
            )
            self._function_caches[key] = cache
            return cache

    def clear_all(self) -> None:
        """Clear all in-memory and on-disk caches."""
        with self._caches_lock:
            self._function_caches = {}

            if os.path.isdir(_CACHE_DIR):
                shutil.rmtree(_CACHE_DIR)


# Singleton MemoCaches instance
_memo_caches = MemoCaches()


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


def call_once(func: Callable[[Any], Any]) -> Any:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if wrapper.was_called:
            raise RuntimeError(f"Cannot call {func.__name__} more than once.")
        result = func(*args, **kwargs)
        wrapper.was_called = True
        return result
    wrapper.was_called = False
    return wrapper



def set_disk_backend(cache_dir: os.PathLike) -> None:
    cache_dir = pathlib.Path(cache_dir)
    if make_cache_path(cache_dir=cache_dir):



class MemoAPI:
    """Implements the public memo API: the `@memo` decorator, and `memo.clear()`."""
    F = TypeVar("F", bound=Callable[..., Any])
    t_lock: threading.Lock = threading.Lock()
    a_lock: asyncio.Lock = asyncio.Lock()

    def __call__(
        self,
        func: Optional[F] = None,
        *,
        persist: bool = False,
        max_entries: Optional[int] = None,
        ttl: Optional[Union[float, timedelta]] = None
    ):
        """Function decorator to memoize function executions.
        
        Memoized data is stored in "pickled" form, which means that the return
        value of a memoized function must be pickleable.
        
        Each caller of a memoized function gets its own copy of the cached data.
        You can clear a memoized function's cache with f.clear().

        This decorator works with both sync and async functions.
        
        Args:
            func: The function to memoize. This hashes the function's source code.
            persist: If `True`, cached values will be persisted to the local disk.
            max_entries: The maximum number of entries to keep in the cache, or
                None for an unbounded cache. (When a new entry is added
                to a full cache, the oldest cached entry will be removed.) The
                default is None.
            ttl: The maximum number of seconds to keep an entry in the cache, or
                None if cache entries should not expire. The default is None.
                Note that ttl is incompatible with `persist=True` - `ttl` will
                be ignored if `persist` is specified.

        Examples:
        >>> @memo
        ... def fetch_and_clean_data(url):
        ...     # Fetch data from URL here, and then clean it up.
        ...     return data
        >>> # Actually executes the function, since this is the first time it was
        ... # encountered.
        ... d1 = fetch_and_clean_data(DATA_URL_1)
        >>> # Does not execute the function. Instead, returns its previously computed
        ... # value. This means that now the data in d1 is the same as in d2.
        ... d2 = fetch_and_clean_data(DATA_URL_1)
        >>> # This is a different URL, so the function executes.
        ... d3 = fetch_and_clean_data(DATA_URL_2)
        >>> # To set the `persist` parameter, use this command as follows
        ... @memo(persist=True)
        ... def fetch_and_clean_data(url):
        ...     # Fetch data from URL here, and then clean it up.
        ...     return data
        >>> # By default, all parameters to a memoized function must be hashable.
        ... # Any parameter whose name begins with `_` will not be hashed. You can use
        ... # this as an "escape hatch" for parameters that are not hashable
        ... @memo
        ... def fetch_and_clean_data(_db_connection, num_rows):
        ...     # Fetch data from _db_connection here, and then clean it up.
        ...     return data
        >>> connection = make_database_connection()
        ... # Actually executes the function, since this is the first time it was
        ... # encountered.
        ... d1 = fetch_and_clean_data(connection, num_rows=10)
        >>> another_connection = make_database_connection()
        ... # Does not execute the function. Instead, returns its previously computed
        ... # value - even though the _database_connection parameter was different
        ... # in both calls.
        ... d2 = fetch_and_clean_data(another_connection, num_rows=10)
        >>> # A memoized function's cache can be procedurally cleared:
        ... @memo
        ... def fetch_and_clean_data(_db_connection, num_rows):
        ...     # Fetch data from _db_connection here, and then clean it up.
        ...     return data
        ... # Clears all cached entries for this function.
        ... fetch_and_clean_data.clear()
        >>> # You can clear also clear all cached entries
        ... memo.clear()
        """
        if not make_cache_path() and persist:
            persist = False

        ttl_seconds: Optional[float]

        if isinstance(ttl, timedelta):
            ttl_seconds = ttl.total_seconds()
        else:
            ttl_seconds = ttl
        
        if func is not None and persist and ttl:
            _LOGGER.warning(
                f"The memoized function '{func.__name__}' has a TTL that will be "
                f"ignored. Persistent memo caches currently don't support TTL."
            )

        if func is None:
            def decorator(f):
                if persist and ttl is not None:
                    _LOGGER.warning(
                        f"The memoized function '{f.__name__}' has a TTL that will be "
                        "ignored. Persistent memo caches currently don't support TTL."
                    )
                
                if inspect.iscoroutinefunction(func):
                    return functools.partial(
                        call_async,
                        f,
                        self.a_lock,
                        persist,
                        max_entries,
                        ttl_seconds
                    )
                return functools.partial(
                        call_sync,
                        f,
                        self.t_lock,
                        persist,
                        max_entries,
                        ttl_seconds
                    )
            
            return decorator
        
        else:
            if inspect.iscoroutinefunction(func):
                return functools.partial(
                    call_async,
                    func,
                    self.a_lock,
                    persist,
                    max_entries,
                    ttl_seconds
                )
            return functools.partial(
                call_sync,
                func,
                self.t_lock,
                persist,
                max_entries,
                ttl_seconds
            )

    @staticmethod
    def clear() -> None:
        """Clear all in-memory and on-disk memo caches."""
        _memo_caches.clear_all()


memo = MemoAPI()