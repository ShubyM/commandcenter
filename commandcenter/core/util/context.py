import asyncio
import concurrent.futures
import functools
from contextvars import ContextVar, copy_context
from typing import Any, Callable, Optional

from commandcenter.core.sources import AvailableSources



ip_address_context: ContextVar[Optional[str]] = ContextVar("ip_address_context", default=None)
user_context: ContextVar[Optional[str]] = ContextVar("user_context", default=None)
source_context: ContextVar[Optional[AvailableSources]] = ContextVar("source_context", default=None)



async def run_in_threadpool_executor_with_context(
    executor: Optional[concurrent.futures.ThreadPoolExecutor],
    func: Callable[[Any], Any],
    *args: Any,
    **kwargs: Any
) -> Any:
    """Execute a sync function in a different thread with current threads context."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        executor,
        copy_context().run,
        functools.partial(func, *args, **kwargs)
    )
