from typing import Any, Dict, Tuple, Type

from commandcenter.caching import singleton
from commandcenter.config import CC_TIMEZONE
from commandcenter.exceptions import NotConfigured
from commandcenter.integrations.protocols import Client, Manager



def resolve_pi_web_client_dependencies(
    _: Manager # TODO: Manager will be used when Redis and RabbitMQ integrations go in
) -> Tuple[Type[Client], Tuple[Any], Dict[str, Any], Dict[str, Any]]:
    """Return all objects for the manager to initialize and support a PI client."""
    from aiohttp import ClientSession, ClientTimeout, TCPConnector

    from commandcenter.config.sources.pi_web import (
        CC_SOURCES_PIWEB_AUTH_DELEGATE,
        CC_SOURCES_PIWEB_AUTH_DOMAIN,
        CC_SOURCES_PIWEB_AUTH_OPPORTUNISTIC,
        CC_SOURCES_PIWEB_AUTH_PASSWORD,
        CC_SOURCES_PIWEB_AUTH_SERVICE,
        CC_SOURCES_PIWEB_AUTH_USERNAME,
        CC_SOURCES_PIWEB_HTTP_KEEPALIVE_TIMEOUT,
        CC_SOURCES_PIWEB_HTTP_REQUEST_TIMEOUT,
        CC_SOURCES_PIWEB_WS_BASE_URL,
        CC_SOURCES_PIWEB_WS_CLOSE_TIMEOUT,
        CC_SOURCES_PIWEB_WS_HEARTBEAT,
        CC_SOURCES_PIWEB_WS_INITIAL_BACKOFF,
        CC_SOURCES_PIWEB_WS_MAX_BACKOFF,
        CC_SOURCES_PIWEB_WS_MAX_BUFFERED_MESSAGES,
        CC_SOURCES_PIWEB_WS_MAX_CONNECTIONS,
        CC_SOURCES_PIWEB_WS_MAX_MESSAGE_SIZE,
        CC_SOURCES_PIWEB_WS_MAX_RECONNECT_ATTEMPTS,
        CC_SOURCES_PIWEB_WS_MAX_SUBSCRIPTIONS,
        CC_SOURCES_PIWEB_WS_PROTOCOLS,
        CC_SOURCES_PIWEB_WS_WEB_ID_TYPE
    )
    from commandcenter.http.aiohttp import NegotiateAuth, create_auth_handlers
    from commandcenter.sources.pi_web import PIWebClient
    
    if not CC_SOURCES_PIWEB_WS_BASE_URL:
        raise NotConfigured("PI web settings not configured.")

    flow = NegotiateAuth(
        username=CC_SOURCES_PIWEB_AUTH_USERNAME,
        password=CC_SOURCES_PIWEB_AUTH_PASSWORD,
        domain=CC_SOURCES_PIWEB_AUTH_DOMAIN,
        service=CC_SOURCES_PIWEB_AUTH_SERVICE,
        delegate=CC_SOURCES_PIWEB_AUTH_DELEGATE,
        opportunistic_auth=CC_SOURCES_PIWEB_AUTH_OPPORTUNISTIC
    )
    request_class, response_class = create_auth_handlers(flow)
    session = ClientSession(
        base_url=CC_SOURCES_PIWEB_WS_BASE_URL,
        connector=TCPConnector(
            limit=None,
            keepalive_timeout=CC_SOURCES_PIWEB_HTTP_KEEPALIVE_TIMEOUT
        ),
        request_class=request_class,
        response_class=response_class,
        timeout=ClientTimeout(total=CC_SOURCES_PIWEB_HTTP_REQUEST_TIMEOUT)
    )

    args = (session, CC_SOURCES_PIWEB_WS_WEB_ID_TYPE)
    kwargs = {
        "max_connections": CC_SOURCES_PIWEB_WS_MAX_CONNECTIONS,
        "max_subscriptions": CC_SOURCES_PIWEB_WS_MAX_SUBSCRIPTIONS,
        "max_buffered_messages": CC_SOURCES_PIWEB_WS_MAX_BUFFERED_MESSAGES,
        "max_reconnect_attempts": CC_SOURCES_PIWEB_WS_MAX_RECONNECT_ATTEMPTS,
        "initial_backoff": CC_SOURCES_PIWEB_WS_INITIAL_BACKOFF,
        "max_backoff": CC_SOURCES_PIWEB_WS_MAX_BACKOFF,
        "protocols": CC_SOURCES_PIWEB_WS_PROTOCOLS,
        "heartbeat": CC_SOURCES_PIWEB_WS_HEARTBEAT,
        "close_timeout": CC_SOURCES_PIWEB_WS_CLOSE_TIMEOUT,
        "max_msg_size": CC_SOURCES_PIWEB_WS_MAX_MESSAGE_SIZE,
        "timezone": CC_TIMEZONE
    }

    return PIWebClient, args, kwargs, {}


@singleton
def setup_pi_http_client():
    """Setup PI Web API HTTP client from the runtime configuration."""
    from aiohttp import ClientSession, ClientTimeout, TCPConnector

    from commandcenter.config.sources.pi_web import (
        CC_SOURCES_PIWEB_AUTH_DELEGATE,
        CC_SOURCES_PIWEB_AUTH_DOMAIN,
        CC_SOURCES_PIWEB_AUTH_OPPORTUNISTIC,
        CC_SOURCES_PIWEB_AUTH_PASSWORD,
        CC_SOURCES_PIWEB_AUTH_SERVICE,
        CC_SOURCES_PIWEB_AUTH_USERNAME,
        CC_SOURCES_PIWEB_HTTP_BASE_URL,
        CC_SOURCES_PIWEB_HTTP_KEEPALIVE_TIMEOUT,
        CC_SOURCES_PIWEB_HTTP_MAX_CONNECTIONS,
        CC_SOURCES_PIWEB_HTTP_REQUEST_TIMEOUT
    )
    from commandcenter.http.aiohttp import NegotiateAuth, create_auth_handlers
    from commandcenter.sources.pi_web import PIWebAPI

    if not CC_SOURCES_PIWEB_HTTP_BASE_URL:
        raise NotConfigured("PI web settings not configured.")

    flow = NegotiateAuth(
        username=CC_SOURCES_PIWEB_AUTH_USERNAME,
        password=CC_SOURCES_PIWEB_AUTH_PASSWORD,
        domain=CC_SOURCES_PIWEB_AUTH_DOMAIN,
        service=CC_SOURCES_PIWEB_AUTH_SERVICE,
        delegate=CC_SOURCES_PIWEB_AUTH_DELEGATE,
        opportunistic_auth=CC_SOURCES_PIWEB_AUTH_OPPORTUNISTIC
    )
    request_class, response_class = create_auth_handlers(flow)
    session = ClientSession(
        base_url=CC_SOURCES_PIWEB_HTTP_BASE_URL,
        connector=TCPConnector(
            limit=CC_SOURCES_PIWEB_HTTP_MAX_CONNECTIONS,
            keepalive_timeout=CC_SOURCES_PIWEB_HTTP_KEEPALIVE_TIMEOUT
        ),
        request_class=request_class,
        response_class=response_class,
        timeout=ClientTimeout(total=CC_SOURCES_PIWEB_HTTP_REQUEST_TIMEOUT)
    )

    return PIWebAPI(session)