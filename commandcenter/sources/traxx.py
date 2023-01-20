from typing import Any, Dict, Tuple, Type

from commandcenter.config import CC_TIMEZONE
from commandcenter.core.integrations.abc import AbstractClient, AbstractManager
from commandcenter.core.objcache import singleton
from commandcenter.exceptions import NotConfigured



def resolve_traxx_stream_client_dependencies(
    manager: AbstractManager #TODO: Manager will be used when Redis and RabbitMQ integrations go in
) -> Tuple[Type[AbstractClient], Tuple[Any], Dict[str, Any], Dict[str, Any]]:
    """Return all objects for the manager to initialize and support a
    `TraxxStreamClient` instance.
    """
    from aiohttp import ClientSession, ClientTimeout, TCPConnector

    from commandcenter.config.sources.traxx import (
        CC_SOURCES_TRAXX_AUTH_FILEPATH,
        CC_SOURCES_TRAXX_HTTP_BASE_URL,
        CC_SOURCES_TRAXX_HTTP_KEEPALIVE_TIMEOUT,
        CC_SOURCES_TRAXX_HTTP_MAX_CONNECTIONS,
        CC_SOURCES_TRAXX_HTTP_REQUEST_TIMEOUT,
        CC_SOURCES_TRAXX_STREAM_BACKOFF_FACTOR,
        CC_SOURCES_TRAXX_STREAM_INITIAL_BACKOFF,
        CC_SOURCES_TRAXX_STREAM_MAX_BUFFERED_MESSAGES,
        CC_SOURCES_TRAXX_STREAM_MAX_MISSED_UPDATES,
        CC_SOURCES_TRAXX_STREAM_MAX_SUBSCRIPTIONS,
        CC_SOURCES_TRAXX_STREAM_UPDATE_INTERVAL
    )
    from commandcenter.core.integrations.util.io.aiohttp import create_auth_handlers
    from commandcenter.core.integrations.util.io.aiohttp.flows import FileCookieAuthFlow
    from commandcenter.core.sources.traxx import TraxxStreamClient

    if not CC_SOURCES_TRAXX_HTTP_BASE_URL:
        raise NotConfigured("Traxx stream settings not configured.")
    
    flow = FileCookieAuthFlow(CC_SOURCES_TRAXX_AUTH_FILEPATH)
    request_class, response_class = create_auth_handlers(flow)
    session = ClientSession(
        base_url=CC_SOURCES_TRAXX_HTTP_BASE_URL,
        connector=TCPConnector(
            limit=CC_SOURCES_TRAXX_HTTP_MAX_CONNECTIONS,
            keepalive_timeout=CC_SOURCES_TRAXX_HTTP_KEEPALIVE_TIMEOUT
        ),
        request_class=request_class,
        response_class=response_class,
        timeout=ClientTimeout(total=CC_SOURCES_TRAXX_HTTP_REQUEST_TIMEOUT)
    )
    
    args = (session,)
    kwargs = {
        "session": session,
        "max_subscriptions": CC_SOURCES_TRAXX_STREAM_MAX_SUBSCRIPTIONS,
        "max_buffered_messages": CC_SOURCES_TRAXX_STREAM_MAX_BUFFERED_MESSAGES,
        "update_interval": CC_SOURCES_TRAXX_STREAM_UPDATE_INTERVAL,
        "max_missed_updates": CC_SOURCES_TRAXX_STREAM_MAX_MISSED_UPDATES,
        "backoff_factor": CC_SOURCES_TRAXX_STREAM_BACKOFF_FACTOR,
        "initial_backoff": CC_SOURCES_TRAXX_STREAM_INITIAL_BACKOFF,
        "timezone": CC_TIMEZONE
    }

    return TraxxStreamClient, args, kwargs, {}


@singleton
async def get_traxx_http_client():
    """Initialize PI Web API HTTP client from the environment configuration.
    
    This can also be used as a dependency.
    """
    from aiohttp import ClientSession, ClientTimeout, TCPConnector

    from commandcenter.config.sources.traxx import (
        CC_SOURCES_TRAXX_AUTH_FILEPATH,
        CC_SOURCES_TRAXX_HTTP_BASE_URL,
        CC_SOURCES_TRAXX_HTTP_KEEPALIVE_TIMEOUT,
        CC_SOURCES_TRAXX_HTTP_MAX_CONNECTIONS,
        CC_SOURCES_TRAXX_HTTP_REQUEST_TIMEOUT
    )
    from commandcenter.core.integrations.util.io.aiohttp import create_auth_handlers
    from commandcenter.core.integrations.util.io.aiohttp.flows import FileCookieAuthFlow
    from commandcenter.core.sources.traxx.http import TraxxClient

    if not CC_SOURCES_TRAXX_HTTP_BASE_URL:
        raise NotConfigured("Traxx stream settings not configured.")
    
    flow = FileCookieAuthFlow(CC_SOURCES_TRAXX_AUTH_FILEPATH)
    request_class, response_class = create_auth_handlers(flow)
    session = ClientSession(
        base_url=CC_SOURCES_TRAXX_HTTP_BASE_URL,
        connector=TCPConnector(
            limit=CC_SOURCES_TRAXX_HTTP_MAX_CONNECTIONS,
            keepalive_timeout=CC_SOURCES_TRAXX_HTTP_KEEPALIVE_TIMEOUT
        ),
        request_class=request_class,
        response_class=response_class,
        timeout=ClientTimeout(total=CC_SOURCES_TRAXX_HTTP_REQUEST_TIMEOUT)
    )

    return TraxxClient(session)