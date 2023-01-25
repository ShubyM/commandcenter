import asyncio
import functools
import inspect
import logging
import math
import os
import pathlib
import shutil
import threading
import types
import warnings
from datetime import timedelta
from typing import Any, Callable, Dict, Optional, Type, TypeVar

try:
    from redis import Redis
except ImportError:
    pass

from commandcenter.caching.core.cache import (
    Cache,
    CachedFunction,
    clear_cached_func,
    wrap_async,
    wrap_sync
)
from commandcenter.caching.core.caches import (
    DiskCache,
    MemoCache,
    RedisCache
)
from commandcenter.caching.core.util import CacheType
from commandcenter.util import ObjSelection
from commandcenter.__version__ import __title__ as DIR_NAME



_LOGGER = logging.getLogger("commandcenter.caching.memo")


class caches(ObjSelection):
    MEMORY = "memory", MemoCache
    DISK = "disk", DiskCache
    REDIS = "redis", RedisCache


class MemoizedFunction(CachedFunction):
    """Implements the `CachedFunction` protocol for `@memo`"""
    def __init__(
        self,
        func: types.FunctionType,
        backend: str,
        max_entries: int | None,
        ttl: float | None,
        **kwargs: Any
    ):
        super().__init__(func)
        self.backend = backend
        self.max_entries = max_entries
        self.ttl = ttl
        self.kwargs = kwargs

    @property
    def cache_type(self) -> CacheType:
        return CacheType.MEMO

    @property
    def display_name(self) -> str:
        """A human-readable name for the cached function"""
        return f"{self.func.__module__}.{self.func.__qualname__}"

    def get_function_cache(self, function_key: str) -> Cache:
        return _memo_caches.get_cache(
            func=self.func,
            key=function_key,
            backend=self.backend,
            max_entries=self.max_entries,
            ttl=self.ttl,
            display_name=self.display_name,
            **self.kwargs
        )


class MemoCaches:
    """Manages all MemoCache instances"""
    def __init__(self):
        self._caches_lock = threading.Lock()
        self._function_caches: Dict[str, MemoCache] = {}

    def get_cache(
        self,
        func: Callable[[Any], Any],
        key: str,
        backend: str | None,
        max_entries: int | float | None,
        ttl: int | float | None,
        display_name: str,
        **kwargs: Any
    ) -> MemoCache:
        """Return the cache for the given key.

        If it doesn't exist, create a new one with the given params.
        """
        backend: Type[Cache] = caches(backend).cls if backend is not None else caches.MEMORY.cls
        if backend is not caches.MEMORY.cls:
            # Limit the buffer size but let entries live forever is ttl is None
            if max_entries is None:
                max_entries = 100
        else:
            max_entries = max_entries or math.inf
        ttl = ttl or math.inf

        # Get the existing cache, if it exists
        with self._caches_lock:
            cache = self._function_caches.get(key)
            if cache is not None:
                assert cache._execution_lock is not None
                return cache

            # Create a new cache object and put it in our dict
            _LOGGER.debug(
                "Creating new MemoCache (key=%s, backend=%s, max_entries=%s, ttl=%s)",
                key,
                backend.__name__,
                max_entries,
                ttl,
            )
            cache = backend(
                key=key,
                max_entries=max_entries,
                ttl=ttl,
                display_name=display_name,
                **kwargs
            )
            if inspect.iscoroutinefunction(func):
                lock = asyncio.Lock()
            else:
                lock = threading.Lock()
            cache.set_lock(lock)
            self._function_caches[key] = cache
            return cache

    def clear_all(self) -> None:
        """Clear all memo caches."""
        with self._caches_lock:
            for cache in self._function_caches.values():
                if not isinstance(cache, caches.DISK.cls):
                    cache.clear()

            if os.path.isdir(memo._cache_dir):
                shutil.rmtree(memo._cache_dir)
            _LOGGER.debug("Cleared all memo caches")

    def __bool__(self) -> bool:
        if self._function_caches:
            return True
        return False


_memo_caches = MemoCaches()


def make_cache_path(cache_dir: pathlib.Path) -> bool:
    """Create the directory(s) required to reach the cache path."""
    try:
        os.makedirs(cache_dir, exist_ok=True)
    except OSError:
        return False
    else:
        return True


class MemoAPI:
    """Implements the public memo API: the `@memo` decorator, and `memo.clear()`."""
    F = TypeVar("F", bound=Callable[..., Any])

    _lock: threading.Lock = threading.Lock()
    _cache_dir: pathlib.Path = pathlib.Path("~").expanduser().joinpath(f".{DIR_NAME}/.cache")
    _redis: "Redis" = None

    def __call__(
        self,
        func: F | None = None,
        *,
        backend: str | None,
        max_entries: int | None = None,
        ttl: float | timedelta | None = None
    ):
        """Function decorator to memoize function executions.
        
        Memoized data is stored in "pickled" form, which means that the return
        value of a memoized function must be pickleable.
        
        Each caller of a memoized function gets its own copy of the cached data.
        You can clear a memoized function's cache with f.clear().

        This decorator works with both sync and async functions.
        
        Args:
            func: The function to memoize. This hashes the function's source code.
            backend: The caching backend to use ("memory", "disk", "redis").
                The default is "memory"
            max_entries: The maximum number of entries to keep in the cache, or
                None for an unbounded cache. (When a new entry is added
                to a full cache, the oldest cached entry will be removed.) The
                default is None. For the "disk" and "redis" backends, an in memory
                cache is used and capped at 100 entries for performance. You can
                disable the in memory buffer by setting `max_entrie` to 0
            ttl: The maximum number of seconds to keep an entry in the cache.
                For the "disk" backend, this only applies to the in memory buffer.
                If using the "redis" backend, ttl cannont be `None`, if `None`
                it defaults to 86400 seconds

        Note: You should always call `memo.clear()` when your appication shuts
        down. This will clear all disk cache files. Its less important with
        "memory" and "redis" backends due to the way those backends works but
        its good practice none the less.

        Examples:
        >>> @memo
        ... def fetch_and_clean_data(url):
        ...     # Fetch data from URL here, and then clean it up.
        ...     return data
        >>> # This actually executes the function, since this is the first time
        >>> # it was encountered.
        >>> d1 = fetch_and_clean_data(DATA_URL_1)
        >>> # This does not execute the function. Instead, returns its previously
        >>> # computed value. This means that now d1 equals d2
        >>> d2 = fetch_and_clean_data(DATA_URL_1)
        >>> # This is a different URL, so the function executes.
        >>> d3 = fetch_and_clean_data(DATA_URL_2)
        
        To set the `backend` parameter, use this command as follows
        >>> @memo(backend="disk")
        ... def fetch_and_clean_data(url):
        ...     # Fetch data from URL here, and then clean it up.
        ...     return data

        There are three available backend options ("memory", "disk", "redis").
        To use the redis backend, run `pip install redis`.

        The disk backend will write cached values to disk. You can configure
        where values are cached by using the `set_cache_dir` method...
        >>> memo.set_cache_dir(...)
        Note: The default caching directory is
        `pathlib.Path("~").expanduser().joinpath(".commandcenter/.cache")`

        If using the redis backend you can pass a configured client instance
        using the `set_redis_client` method...
        >>> memo.set_redis_client(...)

        Both the redis and disk backends maintain a small in memory cache (100 entries)
        with an infinite TTL as a performance buffer. If memory is at a premium
        and you want to disable the buffer simply set `max_entries` to 0 in the
        decorator...
        >>> @memo(backend="disk", max_entries=0)
        ... def fetch_and_clean_data(url):
        ...     # Fetch data from URL here, and then clean it up.
        ...     return data
        Now all calls to `fetch_and_clean` will go to disk to retrieve the cached value.
        
        By default, all parameters to a memoized function must be hashable.
        Any parameter whose name begins with `_` will not be hashed. You can use
        this as an "escape hatch" for parameters that are not hashable...
        >>> @memo
        ... def fetch_and_clean_data(_db_connection, num_rows):
        ...     # Fetch data from _db_connection here, and then clean it up.
        ...     return data
        >>> connection = make_database_connection()
        >>> # Actually executes the function, since this is the first time it was
        >>> # encountered.
        >>> d1 = fetch_and_clean_data(connection, num_rows=10)
        >>> # Does not execute the function. Instead, returns its previously computed
        >>> # value - even though the _database_connection parameter was different
        >>> # in both calls.
        >>> another_connection = make_database_connection()
        >>> d2 = fetch_and_clean_data(another_connection, num_rows=10)
        
        A memoized function's cache can be procedurally cleared (only for "memory"
        backend)...
        ... @memo
        ... def fetch_and_clean_data(_db_connection, num_rows):
        ...     # Fetch data from _db_connection here, and then clean it up.
        ...     return data
        >>> # Clears all cached entries for this function.
        >>> fetch_and_clean_data.clear()
        
        You can clear also clear all cached entries
        >>> memo.clear()

        For HTTP cache-control use cases you can access the TTL property of
        memoize function directly...
        >>> ttl = fetch_and_clean_data.ttl
        """
        backend_kwargs = {}

        backend = caches(backend.lower()) if backend else caches.MEMORY
        if backend is caches.REDIS and self._redis is None:
            self.set_redis_client()
            if self._redis is None:
                backend = caches.MEMORY
            else:
                assert isinstance(self._redis, Redis), "Client not set to Redis instance."
                backend_kwargs["redis"] = lambda: memo._redis
        
        elif backend is caches.DISK:
            if not make_cache_path(self._cache_dir):
                warnings.warn(
                    "Failed to create the cache directory. Memoized objects cannot be "
                    "persisted to disk. Objects will be stored in memory.",
                    stacklevel=2
                )
                backend = caches.MEMORY
            else:
                backend_kwargs["cache_dir"] = lambda: memo._cache_dir

        ttl_seconds: float | None = None

        if isinstance(ttl, timedelta):
            ttl_seconds = ttl.total_seconds()
        else:
            if backend is caches.REDIS and ttl is None:
                warnings.warn(
                    "'ttl' cannot be 'None' when using the redis backend. Setting"
                    "to 86400 seconds",
                    stacklevel=2
                )
                ttl = 86400
            ttl_seconds = ttl

        backend_kwargs.update(
            {
                "backend": backend.value,
                "max_entries": max_entries,
                "ttl": ttl_seconds
            }
        )
        if func is None:
            def decorator(f):
                cached_func = MemoizedFunction(f, **backend_kwargs)
                if inspect.iscoroutinefunction(f):
                    partial = functools.partial(wrap_async, cached_func)
                else:
                    partial = functools.partial(wrap_sync, cached_func)
                partial.clear = functools.partial(clear_cached_func, cached_func)
                if ttl_seconds is None or ttl_seconds > 31536000 or backend is caches.DISK:
                    partial.ttl = 31536000
                else:
                    partial.ttl = ttl_seconds
                return partial
            return decorator
        
        else:
            cached_func = MemoizedFunction(func, **backend_kwargs)
            if inspect.iscoroutinefunction(func):
                partial = functools.partial(wrap_async, cached_func)
            else:
                partial = functools.partial(wrap_sync, cached_func)
            partial.clear = functools.partial(clear_cached_func, cached_func)
            return partial

    @staticmethod
    def clear() -> None:
        """Clear all in-memory and on-disk memo caches."""
        _memo_caches.clear_all()

    @classmethod
    def set_cache_dir(cls, cache_dir: os.PathLike) -> None:
        """Set the caching directory for the memo cache API."""
        with cls._lock:
            with _memo_caches._caches_lock:
                for cache in _memo_caches._function_caches.values():
                    if isinstance(cache, caches.DISK.cls):
                        warnings.warn(
                            "Setting the cache directory while disk caches "
                            "already exist for memoized functions can lead to "
                            "cache misses.",
                            stacklevel=2
                        )
                cache_dir = pathlib.Path(cache_dir)
                if not make_cache_path(cache_dir):
                    warnings.warn(
                        "Failed to create the cache directory. The existing "
                        "directory will be used instead.",
                        stacklevel=2
                    )
                    return
                old_dir = cls._cache_dir
                try:
                    if os.path.isdir(old_dir):
                        shutil.rmtree(old_dir)
                except OSError: # We cant clean up the directory, oh well
                    pass
                cls._cache_dir = cache_dir

    @classmethod
    def set_redis_client(cls, redis: Optional["Redis"] = None) -> None:
        """Set the redis client for the memo cache API."""
        try:
            from redis import Redis
        except ImportError:
            raise RuntimeError(
                "Attempted to use redis support, but the `redis` package is not "
                "installed. Use 'pip install commandcenter[redis]'."
            )
        with cls._lock:
            with _memo_caches._caches_lock:
                for cache in _memo_caches._function_caches.values():
                    if isinstance(cache, caches.REDIS.cls):
                        warnings.warn(
                            "Setting a new Redis client instance while Redis "
                            "caches already exist for memoized functions can "
                            "lead to cache misses.",
                            stacklevel=2
                        )
                if redis is None:
                    cls._redis = Redis()
                else:
                    cls._redis = redis


memo = MemoAPI()