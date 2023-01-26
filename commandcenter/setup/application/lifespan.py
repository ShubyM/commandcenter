import asyncio
import inspect
import logging

import anyio

from commandcenter.caching import iter_singletons, memo, singleton



_LOGGER = logging.getLogger("commandcenter.services.lifespan")


async def on_shutdown_cleanup() -> None:
    """Cleanup application resources ties to `memo` and `singleton` caches."""
    try:
        _ = asyncio.get_running_loop()
        
        closers = [obj for obj in iter_singletons() if hasattr(obj, "close")]
        if closers:
            async_closers = [obj.close() for obj in closers if inspect.iscoroutinefunction(obj.close)]
            sync_closers = [obj.close for obj in closers if not inspect.iscoroutinefunction(obj.close)]
            
            closers = [anyio.to_thread.run_sync(closer) for closer in sync_closers]
            closers.extend(async_closers)
            
            results = await asyncio.gather(*closers, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    _LOGGER.warning("Exception closing resource", exc_info=result)

    except Exception:
        _LOGGER.warning("An error occurred during cleanup", exc_info=True)
    
    finally:
        memo.clear()
        singleton.clear()