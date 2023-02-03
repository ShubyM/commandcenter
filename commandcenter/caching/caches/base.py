import asyncio
import threading
from typing import Any, Union



class BaseCache:
    """Standard function cache interface."""
    def __init__(self) -> None:
        self._execution_lock: Union[threading.Lock, asyncio.Lock] = None

    @property
    def execution_lock(self) -> Union[threading.Lock, asyncio.Lock]:
        return self._execution_lock

    def read_result(self, value_key: str) -> Any:
        """Read a value and associated messages from the cache.
        
        Raises:
          CacheKeyNotFoundError: Raised if value_key is not in the cache.
        """
        raise NotImplementedError()

    def write_result(self, value_key: str, value: Any) -> None:
        """Write a value to the cache, overwriting any existing result that uses
        the value_key.

        Raises:
            CacheError: Raised if unable to write to cache.
        """
        raise NotImplementedError()

    def invalidate(self, value_key: str) -> None:
        """Invalidate a cached value."""
        raise NotImplementedError()

    def clear(self) -> None:
        """Clear all values from this function cache."""
        raise NotImplementedError()

    def set_lock(self, lock: Union[threading.Lock, asyncio.Lock]) -> None:
        """Set the execution lock for the function cache.
        
        The execution lock enforces serial execution of the cached function to
        prevent simultaneous cache misses for the same inputs.
        """
        self._execution_lock = lock