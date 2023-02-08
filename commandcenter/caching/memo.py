import asyncio
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
from typing import Any, Callable, Dict, Optional, Tuple, Type, TypeVar

from commandcenter.caching.caches import Caches
from commandcenter.caching.caches.memo import MemoCache
from commandcenter.caching.core import (
    CacheCollection,
    CachedFunction,
    clear_cached_func,
    create_cached_func_wrapper,
    invalidate_cached_value,
)
from commandcenter.caching.util import CacheType, is_type
from commandcenter.__version__ import __title__ as DIR_NAME



_LOGGER = logging.getLogger("commandcenter.caching.memo")



def configure_memo_cache(
    backend: Callable[[], Caches],
    ttl: int | float | None,
) -> Tuple[Type[MemoCache], Dict[str, Any], int | None]:
    """Configure a memo cache instance for the memo API."""
    backend_kwargs = {}
    backend = backend()

    if backend is Caches.REDIS:
        if not memo._redis:
            set_redis_client(None)
        backend_kwargs["redis"] = lambda: memo._redis
    
    elif backend is Caches.MEMCACHED:
        if not memo._memcached:
            set_memcached_client(None)
        backend_kwargs["memcached"] = lambda: memo._memcached
    
    elif backend is Caches.DISK:
        if not make_cache_path(memo._cache_dir):
            warnings.warn(
                "Failed to create the cache directory. Memoized objects cannot be "
                "persisted to disk. Objects will be stored in memory.",
                stacklevel=2
            )
            backend = Caches.MEMORY
        else:
            backend_kwargs["cache_dir"] = lambda: memo._cache_dir
            
    if backend is Caches.REDIS or backend is Caches.MEMCACHED and ttl is None:
        warnings.warn(
            "'ttl' cannot be 'None' when using the redis | memcached "
            "backends. Setting to 86400 seconds",
            stacklevel=2
        )
        ttl = 86400

    return backend.cls, backend_kwargs, ttl


def set_default_backend(backend: str) -> None:
    """Set the default backend for the memo cache API."""
    with memo._lock:
        memo._default_backend = Caches(backend)


def set_cache_dir(cache_dir: os.PathLike) -> None:
    """Set the caching directory for the memo cache API.
    
    The existing cache directory is deleted.

    Args:
        cache_dir: A pathlike object to store cached objects.
    """
    if not cache_dir:
        return
    with memo._lock:
        with memo_cache_collection._caches_lock:
            for cache in memo_cache_collection._function_caches.values():
                if isinstance(cache, Caches.DISK.cls):
                    warnings.warn(
                        "Setting the cache directory while disk caches "
                        "already exist for memoized functions can lead to "
                        "cache misses.",
                        stacklevel=2
                    )
            
            cache_dir = pathlib.Path(cache_dir)
            old_dir = memo._cache_dir
            if old_dir == cache_dir:
                return

            if not make_cache_path(cache_dir):
                warnings.warn(
                    "Failed to create the cache directory. The existing "
                    "directory will be used instead.",
                    stacklevel=2
                )
                return
            
            try:
                if os.path.isdir(old_dir):
                    shutil.rmtree(old_dir)
            except OSError: # We cant clean up the directory, oh well
                pass
            
            memo._cache_dir = cache_dir


def set_redis_client(redis: Optional["Redis"]) -> None:
    """Set the redis client for the memo cache API.
    
    Args:
        redis: The client instance to use for the memo API. If none a default
            client will be set.
    """
    try:
        from redis import Redis
    except ImportError:
        raise RuntimeError(
            "Attempted to use redis support, but the `redis` package is not "
            "installed. Use 'pip install commandcenter[redis]'."
        )
    with memo._lock:
        with memo_cache_collection._caches_lock:
            for cache in memo_cache_collection._function_caches.values():
                if isinstance(cache, Caches.REDIS.cls):
                    warnings.warn(
                        "Setting a new Redis client instance while Redis "
                        "caches already exist for memoized functions can "
                        "lead to cache misses.",
                        stacklevel=2
                    )
            if redis is None:
                redis = Redis(
                    max_connections=4,
                    socket_timeout=5,
                    socket_connect_timeout=5
                )
            
            if not is_type(redis, "redis.client.Redis"):
                raise TypeError(f"Expected Redis instance, got {type(redis)}.")
            
            memo._redis = redis


def set_memcached_client(memcached: Optional["Memcached"]) -> None:
    """Set the memcached client for the memo cache API."""
    try:
        from pymemcache import PooledClient as Memcached
    except ImportError:
        raise RuntimeError(
            "Attempted to use memcached support, but the `pymemcache` package "
            "is not installed. Use 'pip install commandcenter[memcached]'."
        )
    with memo._lock:
        with memo_cache_collection._caches_lock:
            for cache in memo_cache_collection._function_caches.values():
                if isinstance(cache, Caches.MEMCACHED.cls):
                    warnings.warn(
                        "Setting a new Memcached client instance while Memcached "
                        "caches already exist for memoized functions can "
                        "lead to cache misses.",
                        stacklevel=2
                    )
            if memcached is None:
                memcached = Memcached(
                    ("localhost", 11211),
                    max_pool_size=4,
                    connect_timeout=5,
                    timeout=5
                )
            
            if not is_type(memcached, "pymemcache.client.base.PooledClient"):
                raise TypeError(f"Expected PooledClient instance, got {type(memcached)}.")

            memo._memcached = memcached


def get_cache_control(backend: Callable[[], Caches], ttl: int | None) -> int:
    """Returns the TTL of the cached item in seconds.
    
    This value can be sent along in a cache control header.
    """
    if backend is Caches.REDIS or backend is Caches.MEMCACHED and ttl is None:
        return 86_400
    elif ttl is None or ttl > 31_536_000 or backend is Caches.DISK:
        return 31_536_000
    else:
        return ttl


def make_cache_path(cache_dir: pathlib.Path) -> bool:
    """Create the directory(s) required to reach the cache path."""
    try:
        os.makedirs(cache_dir, exist_ok=True)
    except OSError:
        return False
    else:
        return True


class MemoizedFunction(CachedFunction):
    """Implements the `CachedFunction` protocol for `@memo`"""
    def __init__(
        self,
        func: types.FunctionType,
        backend: Callable[[], Caches],
        max_entries: int | None,
        ttl: float | None
    ):
        super().__init__(func)
        self.backend = backend
        self.max_entries = max_entries
        self.ttl = ttl

    @property
    def cache_type(self) -> CacheType:
        return CacheType.MEMO

    @property
    def display_name(self) -> str:
        """A human-readable name for the cached function"""
        return f"{self.func.__module__}.{self.func.__qualname__}"

    def get_function_cache(self, function_key: str) -> MemoCache:
        return memo_cache_collection.get_cache(
            func=self.func,
            key=function_key,
            backend=self.backend,
            max_entries=self.max_entries,
            ttl=self.ttl,
            display_name=self.display_name
        )


class MemoCacheCollection(CacheCollection):
    """Manages all MemoCache instances"""
    def get_cache(
        self,
        func: Callable[[Any], Any],
        key: str,
        display_name: str,
        backend: Callable[[], Caches],
        max_entries: int | float | None,
        ttl: int | float | None
    ) -> MemoCache:
        """Return the cache for the given key.

        If it doesn't exist, create a new one with the given params.
        """
        backend, kwargs, ttl = configure_memo_cache(backend, ttl)
        if backend is not Caches.MEMORY.cls:
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
                cache.clear()

            if os.path.isdir(memo._cache_dir):
                shutil.rmtree(memo._cache_dir)
            _LOGGER.debug("Cleared all memo caches")

    def __bool__(self) -> bool:
        if self._function_caches:
            return True
        return False


class MemoAPI:
    """Implements the public memo API: the `@memo` decorator, and `memo.clear()`."""
    F = TypeVar("F", bound=Callable[..., Any])

    _lock: threading.Lock = threading.Lock()
    _cache_dir: pathlib.Path = pathlib.Path("~").expanduser().joinpath(f".{DIR_NAME}/.cache")
    _redis: "Redis" = None
    _memcached: "Memcached" = None
    _default_backend: Caches = Caches.MEMORY

    def __call__(
        self,
        func: F | None = None,
        *,
        backend: str | None = None,
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
            backend: The caching backend to use ("memory", "disk", "redis",
                "memcached"). The default is "memory"
            max_entries: The maximum number of entries to keep in the cache, or
                None for an unbounded cache. (When a new entry is added
                to a full cache, the oldest cached entry will be removed.) The
                default is None. For the "disk", "redis", and "memcached" backends,
                an in memory cache is used for performance (100 entries by default).
                You can disable the in memory buffer by setting `max_entrie` to 0.
            ttl: The maximum number of seconds to keep an entry in the cache.
                For the "disk" backend, this only applies to the in memory buffer.
                If using the "redis" or "memcached" backends, ttl cannont be
                `None`, if `None` it defaults to 86400 seconds

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

        There are four available backend options ("memory", "disk", "redis", "memcached").
        The disk backend will write cached values to disk. You can configure
        where values are cached by using the `set_cache_dir` method...
        >>> set_cache_dir(...)
        Note: The default caching directory is
        `pathlib.Path("~").expanduser().joinpath(".commandcenter/.cache")`

        If using the redis backend you can pass a configured client instance
        using the `set_redis_client` method...
        >>> set_redis_client(...)

        If using the memcached backend you can pass a configured client instance
        using the `set_memcached_client` method...
        >>> set_memcached_client(...)

        The redis, memcached, and disk backends maintain a small in memory cache
        (100 entries by default) with an infinite TTL as a performance buffer.
        If memory is at a premium and you want to disable the buffer simply set
        `max_entries` to 0 in the decorator...
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
        
        A memoized function's cache can be procedurally cleared. Note for redis,
        memcached, and disk caches, this only clears the memory buffer...
        >>> @memo
        ... def fetch_and_clean_data(_db_connection, num_rows):
        ...     # Fetch data from _db_connection here, and then clean it up.
        ...     return data
        >>> # Clears all cached entries for this function.
        >>> fetch_and_clean_data.clear()

        A cached value can be procedurally invalidated by calling the `invalidate`
        method...
        >>> @memo
        ... def fetch_and_clean_data(_db_connection, num_rows):
        ...     # Fetch data from _db_connection here, and then clean it up.
        ...     return data
        >>> # Invalidate a cached value
        >>> fetch_and_clean_data.invalidate(another_connection, num_rows=10)
        
        You can clear also clear all cached entries. Note for redis and memcached
        caches, this only clears the memory buffer...
        >>> memo.clear()

        For HTTP cache-control use cases you can access the TTL property of
        memoize function directly...
        >>> ttl = fetch_and_clean_data.ttl
        """
        if backend is not None:
            backend_callable = lambda: Caches(backend)
        else:
            backend_callable = lambda: self._default_backend

        if isinstance(ttl, timedelta):
            ttl_seconds = ttl.total_seconds()
        else:
            ttl_seconds = ttl
        
        if func is None:
            def decorator(f):
                cached_func = MemoizedFunction(
                    f,
                    backend=backend_callable,
                    max_entries=max_entries,
                    ttl=ttl_seconds
                )
                wrapper = create_cached_func_wrapper(f, cached_func)
                wrapper.clear = clear_cached_func(cached_func)
                wrapper.invalidate = invalidate_cached_value(cached_func)
                wrapper.ttl = lambda: get_cache_control(backend, ttl_seconds)
                return wrapper
            return decorator
        
        else:
            cached_func = MemoizedFunction(
                func,
                backend=backend_callable,
                max_entries=max_entries,
                ttl=ttl_seconds
            )
            wrapper = create_cached_func_wrapper(func, cached_func)
            wrapper.clear = clear_cached_func(cached_func)
            wrapper.invalidate = invalidate_cached_value(cached_func)
            wrapper.ttl = lambda: get_cache_control(backend, ttl_seconds)
            return wrapper

    @staticmethod
    def clear() -> None:
        """Clear all in-memory and on-disk memo caches."""
        memo_cache_collection.clear_all()


memo_cache_collection = MemoCacheCollection()
memo = MemoAPI()