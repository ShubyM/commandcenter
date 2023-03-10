import asyncio
import inspect
import logging
import threading
from collections.abc import Iterable
from typing import Any, Callable, TypeVar

from commandcenter.caching.caches.singleton import SingletonCache
from commandcenter.caching.core import (
    CacheCollection,
    CachedFunction,
    clear_cached_func,
    create_cached_func_wrapper
)
from commandcenter.caching.util import CacheType



_LOGGER = logging.getLogger("commandcenter.caching.singleton")


def iter_singletons() -> Iterable[Any]:
    """Iterate over all singleton objects in the cache.
    
    This can be useful in 'shutdown' functions when you want to make sure all
    resources in cached instances are cleaned up appropriately.
    """
    for obj in singleton_cache_collection:
        yield obj


class SingletonFunction(CachedFunction):
    """Implements the `CachedFunction` protocol for `@singleton`"""
    @property
    def cache_type(self) -> CacheType:
        """The cache type for this function."""
        return CacheType.SINGLETON

    @property
    def display_name(self) -> str:
        """A human-readable name for the cached function"""
        return f"{self.func.__module__}.{self.func.__qualname__}"

    def get_function_cache(self, function_key: str) -> SingletonCache:
        """Get or create the function cache for the given key."""
        return singleton_cache_collection.get_cache(
            func=self.func,
            key=function_key,
            display_name=self.display_name
        )


class SingletonCaches(CacheCollection, Iterable[Any]):
    """Manages all `SingletonCache` instances"""
    def get_cache(
        self,
        func: Callable[[Any], Any],
        key: str,
        display_name: str
    ) -> "SingletonCache":
        """Return the mem cache for the given key.
        
        If it doesn't exist, create a new one with the given params.
        """
        # Get the existing cache, if it exists
        with self._caches_lock:
            cache = self._function_caches.get(key)
            if cache is not None:
                assert cache._execution_lock is not None
                return cache

            # Create a new cache object and put it in our dict
            _LOGGER.debug("Creating new SingletonCache (key=%s)", key)
            cache = SingletonCache(
                key=key,
                display_name=display_name
            )
            if inspect.iscoroutinefunction(func):
                lock = asyncio.Lock()
            else:
                lock = threading.Lock()
            cache.set_lock(lock)
            self._function_caches[key] = cache
            return cache

    def clear_all(self) -> None:
        """Clear all singleton caches."""
        with self._caches_lock:
            self._function_caches = {}

    def __iter__(self) -> Iterable[Any]:
        with self._caches_lock:
            for cache in self._function_caches.values():
                with cache._mem_cache_lock:
                    for obj in cache._mem_cache.values():
                        yield obj


class SingletonAPI:
    """Implements the public singleton API: the `@singleton` decorator,
    and `singleton.clear()`.
    """
    F = TypeVar("F", bound=Callable[..., Any])

    def __call__(self, func: F | None = None):
        """Function decorator to store singleton objects.
        
        Each singleton object is shared across all threads in the application.
        Singleton objects must be thread-safe, because they can be accessed from
        multiple threads concurrently.

        This decorator works with both sync and async functions.
        
        Args:
            func: The function that creates the singleton. This hashes the
                function's source code.

        Examples:
        >>> @singleton
        ... def get_database_session(url):
        ...     # Create a database session object that points to the URL.
        ...     return session
        >>> # This actually executes the function, since this is the first time
        >>> # it was encountered...
        >>> s1 = get_database_session(SESSION_URL_1)
        >>> # This does not execute the function. Instead, returns its previously
        >>> # computed value. This means that now the connection object in s1 is
        >>> # the same as in s2...
        >>> s2 = get_database_session(SESSION_URL_1)
        >>> assert id(s1) == id(s2)
        >>> # This is a different URL, so the function executes...
        >>> s3 = get_database_session(SESSION_URL_2)
        
        By default, all parameters to a singleton function must be hashable.
        Any parameter whose name begins with `_` will not be hashed. You can use
        this as an "escape hatch" for parameters that are not hashable...
        >>> @singleton
        ... def get_database_session(_sessionmaker, url):
        ...     # Create a database connection object that points to the URL.
        ...     return connection
        >>> # This actually executes the function, since this is the first time
        >>> # it was encountered...
        >>> s1 = get_database_session(SESSION_URL_1)
        >>> # This does not execute the function. Instead, returns its previously
        >>> # computed value - even though the _sessionmaker parameter was
        >>> different in both calls...
        >>> s2 = get_database_session(create_sessionmaker(), DATA_URL_1)
        >>> assert id(s1) == id(s2)
        
        A singleton function's cache can be procedurally cleared...
        >>> @singleton
        ... def get_database_session(_sessionmaker, url):
        ...     # Create a database connection object that points to the URL.
        ...     return connection
        >>> # Clear all cached entries for this function.
        >>> get_database_session.clear()
        >>> # You can also clear all cached entries
        >>> singleton.clear()
        """
        if func is None:
            def decorator(f):
                cached_func = SingletonFunction(f)
                wrapper = create_cached_func_wrapper(f, cached_func)
                wrapper.clear = clear_cached_func(cached_func)
                return wrapper
            return decorator
        
        else:
            cached_func = SingletonFunction(func)
            wrapper = create_cached_func_wrapper(func, cached_func)
            wrapper.clear = clear_cached_func(cached_func)
            return wrapper

    @staticmethod
    def clear() -> None:
        """Clear all singleton caches."""
        singleton_cache_collection.clear_all()


singleton_cache_collection = SingletonCaches()
singleton = SingletonAPI()