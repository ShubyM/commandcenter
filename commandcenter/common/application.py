import asyncio
import inspect
import logging
from collections.abc import Coroutine, Sequence
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Type,
    Union,
)

from fastapi import Depends, FastAPI, Request, Response
from fastapi.middleware import Middleware

from commandcenter.common.logging import setup_logging, setup_sentry
from commandcenter.common.middleware import setup_middleware
from commandcenter.common.routes import admin_, users_
from commandcenter.config import CC_DEBUG_MODE, CC_OBJCACHE_DIR
from commandcenter.core.objcache import iter_singletons, memo, set_cache_dir, singleton



_LOGGER = logging.getLogger("commandcenter.services")


async def cleanup_resources() -> None:
    """Cleanup all resources associate to singlton cache objects and clear both
    the singleton and memo caches.
    """
    try:
        loop = asyncio.get_running_loop()
        closers = [obj for obj in iter_singletons() if hasattr(obj, "close")]
        if closers:
            async_closers = [obj.close() for obj in closers if inspect.iscoroutinefunction(obj.close)]
            sync_closers = [obj.close for obj in closers if not inspect.iscoroutinefunction(obj.close)]
            closers = [loop.run_in_executor(closer) for closer in sync_closers]
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


def setup_application(
    title: str,
    description: str,
    version: str,
    include_admin_route: bool = True,
    dependencies: Optional[Sequence[Depends]] = None,
    middleware: Optional[Sequence[Middleware]] = None,
    exception_handlers: Optional[
        Dict[
            Union[int, Type[Exception]],
            Callable[[Request, Any], Coroutine[Any, Any, Response]],
        ]
    ] = None,
    on_startup: Optional[Sequence[Callable[[], Any]]] = None,
    on_shutdown: Optional[Sequence[Callable[[], Any]]] = None,
    root_path: str = ""
) -> FastAPI:

    default_middlware = setup_middleware()
    middleware = middleware or []
    middleware.extend(default_middlware)

    on_shutdown = on_shutdown or []
    on_shutdown.extend([cleanup_resources])

    app = FastAPI(
        debug=CC_DEBUG_MODE,
        title=title,
        description=description,
        version=version,
        dependencies=dependencies,
        middleware=middleware,
        exception_handlers=exception_handlers,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        root_path=root_path
    )

    if include_admin_route:
        app.include_router(admin_)
    app.include_router(users_)

    setup_logging()
    setup_sentry()
    set_cache_dir(CC_OBJCACHE_DIR)

    return app