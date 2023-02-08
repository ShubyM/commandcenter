import logging
from contextlib import AsyncExitStack
from typing import Any

from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpcore import AsyncConnectionPool
from httpx import AsyncHTTPTransport, Limits, Timeout, URL

from commandcenter.auth import BaseUser
from commandcenter.events import (
    Event,
    EventQueryResult,
    Topic,
    TopicQueryResult,
    TopicSubscription,
    TopicSubscriptionRequest
)
from commandcenter.client.base import CommandCenterHttpxClient, app_lifespan_context
from commandcenter.util import Status, StatusOptions



_LOGGER = logging.getLogger("commandcenter.client")


class CommandCenterClient:
    def __init__(
        self,
        api: str | FastAPI,
        **httpx_settings: Any
    ) -> None:
        httpx_settings = httpx_settings.copy() if httpx_settings else {}
        httpx_settings.setdefault("headers", {})

        # Context management
        self._exit_stack = AsyncExitStack()
        self._ephemeral_app: FastAPI | None = None
        self.manage_lifespan = True

        # Only set if this client started the lifespan of the application
        self._ephemeral_lifespan: LifespanManager | None = None
        self._closed = False
        self._started = False

        # Connect to an external application
        if isinstance(api, str):
            if httpx_settings.get("app"):
                raise ValueError(
                    "Invalid httpx settings: `app` cannot be set when providing an "
                    "api url. `app` is only for use with ephemeral instances. Provide "
                    "it as the `api` parameter instead."
                )
            httpx_settings.setdefault("base_url", api)

            # See https://www.python-httpx.org/advanced/#pool-limit-configuration
            httpx_settings.setdefault(
                "limits",
                Limits(
                    max_connections=16,
                    max_keepalive_connections=8,
                    # Only allow the client to keep connections alive for 25s.
                    keepalive_expiry=25,
                ),
            )
            self.api_url = api

        # Connect to an in-process application
        elif isinstance(api, FastAPI):
            self._ephemeral_app = api
            httpx_settings.setdefault("app", self._ephemeral_app)
            httpx_settings.setdefault("base_url", "http://ephemeral-commandcenter/api")

        else:
            raise TypeError(
                f"Unexpected type {type(api).__name__!r} for argument `api`. Expected 'str' or 'FastAPI'"
            )

        # See https://www.python-httpx.org/advanced/#timeout-configuration
        httpx_settings.setdefault(
            "timeout",
            Timeout(
                connect=30,
                read=30,
                write=30,
                pool=30,
            ),
        )

        self._client = CommandCenterHttpxClient(
            **httpx_settings,
        )

        # See https://www.python-httpx.org/advanced/#custom-transports
        #
        # If we're using an HTTP/S client (not the ephemeral client), adjust the
        # transport to add retries _after_ it is instantiated. If we alter the transport
        # before instantiation, the transport will not be aware of proxies unless we
        # reproduce all of the logic to make it so.
        #
        # Only alter the transport to set our default of 3 retries, don't modify any
        # transport a user may have provided via httpx_settings.
        #
        # Making liberal use of getattr and isinstance checks here to avoid any
        # surprises if the internals of httpx or httpcore change on us
        if isinstance(api, str) and not httpx_settings.get("transport"):
            transport_for_url = getattr(self._client, "_transport_for_url", None)
            if callable(transport_for_url):
                orion_transport = transport_for_url(URL(api))
                if isinstance(orion_transport, AsyncHTTPTransport):
                    pool = getattr(orion_transport, "_pool", None)
                    if isinstance(pool, AsyncConnectionPool):
                        pool._retries = 3

    async def whoami(self) -> BaseUser:
        """Send a GET request to /users/whoami."""
        response = await self._client.get("/users/whoami")
        content = await response.json()
        return BaseUser(**content)

    async def create_event_topic(self, topic: Topic) -> None:
        response = await self._client.post("/events/topics/save", json=topic.json())

    async def __aenter__(self):
        """Start the client.
        
        If the client is already started, this will raise an exception.
        
        If the client is already closed, this will raise an exception. Use a new client
        instance instead.
        """
        if self._closed:
            # httpx.AsyncClient does not allow reuse so we will not either.
            raise RuntimeError(
                "The client cannot be started again after closing. "
                "Retrieve a new client with `get_client()` instead."
            )

        if self._started:
            # httpx.AsyncClient does not allow reentrancy so we will not either.
            raise RuntimeError("The client cannot be started more than once.")

        await self._exit_stack.__aenter__()

        # Enter a lifespan context if using an ephemeral application.
        # See https://github.com/encode/httpx/issues/350
        if self._ephemeral_app and self.manage_lifespan:
            self._ephemeral_lifespan = await self._exit_stack.enter_async_context(
                app_lifespan_context(self._ephemeral_app)
            )

        if self._ephemeral_app:
            _LOGGER.debug("Using ephemeral application")
        else:
            _LOGGER.debug("Connecting to API at %s", self.api_url)

        # Enter the httpx client's context
        await self._exit_stack.enter_async_context(self._client)

        self._started = True

        return self

    async def __aexit__(self, *exc_info):
        """Shutdown the client."""
        self._closed = True
        return await self._exit_stack.__aexit__(*exc_info)

    def __enter__(self):
        raise RuntimeError(
            "The `CommandCenterClient` must be entered with an async context. Use 'async "
            "with CommandCenterClient(...)' not 'with CommandCenterClient(...)'"
        )

    def __exit__(self, *_):
        assert False, "This should never be called but must be defined for __enter__"