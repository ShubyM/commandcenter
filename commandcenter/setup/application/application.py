import os
from collections.abc import Coroutine, Sequence
from typing import Any, Callable, Dict, Type

from fastapi import Depends, FastAPI, Request, Response
from starlette.middleware import Middleware

from commandcenter.config import CC_DEBUG_MODE, CC_HOME
from commandcenter.auth.debug import enable_interactive_auth
from commandcenter.setup.application.lifespan import on_shutdown_cleanup
from commandcenter.setup.caching import setup_caching
from commandcenter.setup.logging import configure_logging
from commandcenter.setup.middleware import configure_middleware
from commandcenter.setup.sentry import configure_sentry


def setup_application(
    title: str,
    description: str,
    version: str,
    dependencies: Sequence[Depends] | None = None,
    middleware: Sequence[Middleware] | None = None,
    exception_handlers: Dict[
        int | Type[Exception],
        Callable[[Request, Any], Coroutine[Any, Any, Response]]
    ] | None = None,
    on_startup: Sequence[Callable[[], Any]] | None = None,
    on_shutdown: Sequence[Callable[[], Any]] | None = None,
    root_path: str = ""
) -> FastAPI:
    """Configure and return a FastAPI application instance."""
    os.makedirs(CC_HOME, exist_ok=True)
    default_middlware = configure_middleware()
    middleware = middleware or []
    middleware.extend(default_middlware)

    on_shutdown = on_shutdown or []
    on_shutdown.extend([on_shutdown_cleanup])

    if CC_DEBUG_MODE:
        dependencies = dependencies or []
        dependencies.append(Depends(enable_interactive_auth))

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

    configure_logging()
    configure_sentry()
    setup_caching()

    return app