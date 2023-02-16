from typing import Any, Dict, Tuple, Type

from commandcenter.config import CC_TIMEZONE
from commandcenter.caching import singleton
from commandcenter.exceptions import NotConfigured
from commandcenter.integrations import Client, Manager, Managers



def resolve_traxx_client_dependencies(
    manager: Manager
) -> Tuple[Type[Client], Tuple[Any], Dict[str, Any], Dict[str, Any]]:
    """Return all objects for the manager to initialize and support a Traxx client.
    
    This must be run in the same thread as the event loop.
    """
    from aiohttp import ClientSession, ClientTimeout, TCPConnector

    from commandcenter.config.sources.traxx import (
        CC_SOURCES_TRAXX_AUTH_FILEPATH,
        CC_SOURCES_TRAXX_CHANNEL_NAME,
        CC_SOURCES_TRAXX_HTTP_BASE_URL,
        CC_SOURCES_TRAXX_HTTP_KEEPALIVE_TIMEOUT,
        CC_SOURCES_TRAXX_HTTP_MAX_CONNECTIONS,
        CC_SOURCES_TRAXX_HTTP_REQUEST_TIMEOUT,
        CC_SOURCES_TRAXX_STREAM_INITIAL_BACKOFF,
        CC_SOURCES_TRAXX_STREAM_MAX_BUFFERED_MESSAGES,
        CC_SOURCES_TRAXX_STREAM_MAX_MISSED_UPDATES,
        CC_SOURCES_TRAXX_STREAM_MAX_SUBSCRIPTIONS,
        CC_SOURCES_TRAXX_STREAM_UPDATE_INTERVAL
    )
    from commandcenter.util import FileCookieAuthFlow, create_auth_handlers
    from commandcenter.sources.traxx import TraxxClient

    if not CC_SOURCES_TRAXX_HTTP_BASE_URL:
        raise NotConfigured("Traxx settings not configured.")
    try:
        flow = FileCookieAuthFlow(CC_SOURCES_TRAXX_AUTH_FILEPATH)
    except (FileNotFoundError, ValueError) as err:
        raise NotConfigured("Traxx settings not configured.") from err

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
        "max_subscriptions": CC_SOURCES_TRAXX_STREAM_MAX_SUBSCRIPTIONS,
        "max_buffered_messages": CC_SOURCES_TRAXX_STREAM_MAX_BUFFERED_MESSAGES,
        "update_interval": CC_SOURCES_TRAXX_STREAM_UPDATE_INTERVAL,
        "max_missed_updates": CC_SOURCES_TRAXX_STREAM_MAX_MISSED_UPDATES,
        "initial_backoff": CC_SOURCES_TRAXX_STREAM_INITIAL_BACKOFF,
        "timezone": CC_TIMEZONE
    }
    add_kwargs = {}
    if manager is Managers.REDIS.cls:
        add_kwargs["channel"] = CC_SOURCES_TRAXX_CHANNEL_NAME
    elif manager is Managers.RABBITMQ.cls:
        add_kwargs["exchange"] = CC_SOURCES_TRAXX_CHANNEL_NAME

    return TraxxClient, args, kwargs, add_kwargs


@singleton
def setup_traxx_http_client():
    """Configure a Traxx HTTP client from the environment.
    
    This must be run in the same thread as the event loop.
    """
    from aiohttp import ClientSession, ClientTimeout, TCPConnector

    from commandcenter.config.sources.traxx import (
        CC_SOURCES_TRAXX_AUTH_FILEPATH,
        CC_SOURCES_TRAXX_HTTP_BASE_URL,
        CC_SOURCES_TRAXX_HTTP_KEEPALIVE_TIMEOUT,
        CC_SOURCES_TRAXX_HTTP_MAX_CONNECTIONS,
        CC_SOURCES_TRAXX_HTTP_REQUEST_TIMEOUT
    )
    from commandcenter.util import FileCookieAuthFlow, create_auth_handlers
    from commandcenter.sources.traxx import TraxxAPI

    if not CC_SOURCES_TRAXX_HTTP_BASE_URL:
        raise NotConfigured("Traxx settings not configured.")
    
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

    return TraxxAPI(session)