from typing import Any, Dict, Tuple, Type

from commandcenter.config import CC_TIMEZONE
from commandcenter.core.integrations.abc import AbstractClient, AbstractManager
from commandcenter.core.objcache import singleton
from commandcenter.exceptions import NotConfigured


def resolve_pi_channel_client_dependencies(
    manager: AbstractManager # TODO: Manager will be used when Redis and RabbitMQ integrations go in
) -> Tuple[Type[AbstractClient], Tuple[Any], Dict[str, Any], Dict[str, Any]]:
    """Return all objects for the manager to initialize and support a
    `PIChannelClient` instance.
    """
    from aiohttp import ClientSession, ClientTimeout, TCPConnector

    from commandcenter.config.sources.pi_web import (
        CC_SOURCES_PIWEB_AUTH_DELEGATE,
        CC_SOURCES_PIWEB_AUTH_DOMAIN,
        CC_SOURCES_PIWEB_AUTH_OPPORTUNISTIC,
        CC_SOURCES_PIWEB_AUTH_PASSWORD,
        CC_SOURCES_PIWEB_AUTH_SERVICE,
        CC_SOURCES_PIWEB_AUTH_USERNAME,
        CC_SOURCES_PIWEB_CHANNEL_BACKOFF_FACTOR,
        CC_SOURCES_PIWEB_CHANNEL_BASE_URL,
        CC_SOURCES_PIWEB_CHANNEL_CLOSE_TIMEOUT,
        CC_SOURCES_PIWEB_CHANNEL_HEARTBEAT,
        CC_SOURCES_PIWEB_CHANNEL_INITIAL_BACKOFF,
        CC_SOURCES_PIWEB_CHANNEL_MAX_BACKOFF,
        CC_SOURCES_PIWEB_CHANNEL_MAX_BUFFERED_MESSAGES,
        CC_SOURCES_PIWEB_CHANNEL_MAX_CONNECTIONS,
        CC_SOURCES_PIWEB_CHANNEL_MAX_MESSAGE_SIZE,
        CC_SOURCES_PIWEB_CHANNEL_MAX_RECONNECT_ATTEMPTS,
        CC_SOURCES_PIWEB_CHANNEL_MAX_SUBSCRIPTIONS,
        CC_SOURCES_PIWEB_CHANNEL_PROTOCOLS,
        CC_SOURCES_PIWEB_CHANNEL_WEB_ID_TYPE,
        CC_SOURCES_PIWEB_HTTP_KEEPALIVE_TIMEOUT,
        CC_SOURCES_PIWEB_HTTP_REQUEST_TIMEOUT
    )
    from commandcenter.core.integrations.util.io.aiohttp import create_auth_handlers
    from commandcenter.core.integrations.util.io.aiohttp.flows import NegotiateAuth
    from commandcenter.core.sources.pi_web import PIChannelClient
    
    if not CC_SOURCES_PIWEB_CHANNEL_BASE_URL:
        raise NotConfigured("PI channel settings not configured.")

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
        base_url=CC_SOURCES_PIWEB_CHANNEL_BASE_URL,
        connector=TCPConnector(
            limit=None,
            keepalive_timeout=CC_SOURCES_PIWEB_HTTP_KEEPALIVE_TIMEOUT
        ),
        request_class=request_class,
        response_class=response_class,
        timeout=ClientTimeout(total=CC_SOURCES_PIWEB_HTTP_REQUEST_TIMEOUT)
    )

    args = (session, CC_SOURCES_PIWEB_CHANNEL_WEB_ID_TYPE)
    kwargs = {
        "max_connections": CC_SOURCES_PIWEB_CHANNEL_MAX_CONNECTIONS,
        "max_subscriptions": CC_SOURCES_PIWEB_CHANNEL_MAX_SUBSCRIPTIONS,
        "max_buffered_messages": CC_SOURCES_PIWEB_CHANNEL_MAX_BUFFERED_MESSAGES,
        "max_reconnect_attempts": CC_SOURCES_PIWEB_CHANNEL_MAX_RECONNECT_ATTEMPTS,
        "backoff_factor": CC_SOURCES_PIWEB_CHANNEL_BACKOFF_FACTOR,
        "initial_backoff": CC_SOURCES_PIWEB_CHANNEL_INITIAL_BACKOFF,
        "max_backoff": CC_SOURCES_PIWEB_CHANNEL_MAX_BACKOFF,
        "protocols": CC_SOURCES_PIWEB_CHANNEL_PROTOCOLS,
        "heartbeat": CC_SOURCES_PIWEB_CHANNEL_HEARTBEAT,
        "close_timeout": CC_SOURCES_PIWEB_CHANNEL_CLOSE_TIMEOUT,
        "max_msg_size": CC_SOURCES_PIWEB_CHANNEL_MAX_MESSAGE_SIZE,
        "timezone": CC_TIMEZONE
    }

    return PIChannelClient, args, kwargs, {}


@singleton
async def build_pi_http_client():
    """Initialize PI Web API HTTP client from the environment configuration."""
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
    from commandcenter.core.integrations.util.io.aiohttp import create_auth_handlers
    from commandcenter.core.integrations.util.io.aiohttp.flows import NegotiateAuth
    from commandcenter.core.sources.pi_web.http import PIWebClient

    if not CC_SOURCES_PIWEB_HTTP_BASE_URL:
        raise NotConfigured("PI client settings not configured.")

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

    return PIWebClient(session)


async def get_pi_http_client():
    """Dependency for building an PI Web HTTP client."""
    return await build_pi_http_client()