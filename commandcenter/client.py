import asyncio
import functools
import json
import logging
import os
import pathlib
import sys
import tempfile
from collections.abc import AsyncGenerator, Awaitable
from contextlib import AsyncExitStack
from datetime import datetime
from typing import Any, Callable, Dict, Set, Tuple, Type

import pandas as pd
from fastapi import FastAPI, status
from httpcore import AsyncConnectionPool
from httpx import (
    AsyncClient,
    AsyncHTTPTransport,
    Limits,
    LocalProtocolError,
    PoolTimeout,
    QueryParams,
    ReadError,
    ReadTimeout,
    RemoteProtocolError,
    Response,
    Timeout,
    WriteError,
    URL
)
from pydantic import BaseModel, ValidationError

from commandcenter.auth import BaseUser
from commandcenter.caching.tokens import ReferenceToken
from commandcenter.events import (
    Event,
    EventQueryResult,
    Topic,
    TopicQueryResult,
    TopicSubscription,
    TopicSubscriptionRequest
)
from commandcenter.integrations import AnySubscriberMessage
from commandcenter.timeseries import (
    TimeseriesSamples,
    UnitOp,
    UnitOpQueryResult
)
from commandcenter.util import SSEParser, Status



_LOGGER = logging.getLogger("commandcenter.client")


class CCHttpxClient(AsyncClient):
    """A wrapper for the async httpx client with support for retry-after
    headers for:
        - 503 Service unavailable

    Additionally, this client will always call `raise_for_status` on responses.
    """
    RETRY_MAX = 5

    async def _send_with_retry(
        self,
        request: Callable[[], Awaitable[Response]],
        retry_codes: Set[int] = set(),
        retry_exceptions: Tuple[Type[Exception], ...] = tuple(),
    ) -> Response:
        """Send a request and retry it if it fails.

        Sends the provided request and retries it up to self.RETRY_MAX times if
        the request either raises an exception listed in `retry_exceptions` or receives
        a response with a status code listed in `retry_codes`.

        Retries will be delayed based on either the retry header (preferred) or
        exponential backoff if a retry header is not provided.
        """
        try_count = 0
        response = None

        while try_count <= self.RETRY_MAX:
            try_count += 1
            retry_seconds = None
            exc_info = None

            try:
                response = await request()
            except retry_exceptions:
                if try_count > self.RETRY_MAX:
                    raise
                # Otherwise, we will ignore this error but capture the info for logging
                exc_info = sys.exc_info()
            else:
                # We got a response; return immediately if it is not retryable
                if response.status_code not in retry_codes:
                    return response

                if "Retry-After" in response.headers:
                    retry_seconds = float(response.headers["Retry-After"])

            # Use an exponential back-off if not set in a header
            if retry_seconds is None:
                retry_seconds = 2**try_count

            _LOGGER.debug(
                (
                    "Encountered retryable exception during request. "
                    if exc_info
                    else "Received response with retryable status code. "
                )
                + (
                    f"Another attempt will be made in {retry_seconds}s. "
                    f"This is attempt {try_count}/{self.RETRY_MAX + 1}."
                ),
                exc_info=exc_info,
            )
            await asyncio.sleep(retry_seconds)

        assert (
            response is not None
        ), "Retry handling ended without response or exception"

        # We ran out of retries, return the failed response
        return response

    async def send(self, *args, **kwargs) -> Response:
        api_request = functools.partial(super().send, *args, **kwargs)

        response = await self._send_with_retry(
            request=api_request,
            retry_codes={
                status.HTTP_503_SERVICE_UNAVAILABLE,
            },
            retry_exceptions=(
                ReadTimeout,
                PoolTimeout,
                # `ConnectionResetError` when reading socket raises as a `ReadError`
                ReadError,
                # Sockets can be closed during writes resulting in a `WriteError`
                WriteError,
                # Uvicorn bug, see https://github.com/PrefectHQ/prefect/issues/7512
                RemoteProtocolError,
                # HTTP2 bug, see https://github.com/PrefectHQ/prefect/issues/7442
                LocalProtocolError,
            ),
        )

        # Always raise bad responses
        response.raise_for_status()

        return response


class CCClient:
    def __init__(
        self,
        api: str | FastAPI,
        **httpx_settings: Any
    ) -> None:
        httpx_settings = httpx_settings.copy() if httpx_settings else {}
        httpx_settings.setdefault("headers", {})

        self._exit_stack = AsyncExitStack()
        self._closed = False
        self._started = False

        if httpx_settings.get("app"):
            raise ValueError(
                "Invalid httpx settings: `app` cannot be set when providing an "
                "api url."
            )
        httpx_settings.setdefault("base_url", api)
        httpx_settings.setdefault(
            "limits",
            Limits(max_connections=25, max_keepalive_connections=10, keepalive_expiry=25)
        )
        self.api_url = api

        httpx_settings.setdefault(
            "timeout",
            Timeout(connect=30, read=30, write=30, pool=30)
        )

        self._client = CCHttpxClient(
            **httpx_settings,
        )

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
        return await self._get("/users/whoami", BaseUser)

    async def create_event_topic(self, topic: Topic) -> Status:
        """Send a POST request to /events/topics/save."""
        data = topic.json()
        return await self._post("/events/topics/save", Status, data)
    
    async def get_topic(self, topic: str) -> Topic:
        """Send a GET request to /events/topics/{topic}."""
        return await self._get(f"/events/topics/{topic}", Topic)
    
    async def get_topics(self) -> TopicQueryResult:
        """Set a GET request to /events/topics."""
        return await self._get("/events/topics", TopicQueryResult)
    
    async def publish_event(self, event: Event) -> Status:
        """Send a POST request to /events/publish/{event.topic}."""
        data = event.json()
        return await self._post(f"/events/publish/{event.topic}", Status, data)
    
    async def subscribe_to_topics(self, *topics: TopicSubscription) -> ReferenceToken:
        """Send a POST request to /events/subscribe."""
        data = TopicSubscriptionRequest(subscriptions=topics).json()
        return await self._post("/events/subscribe", ReferenceToken, data)
    
    async def get_last_event(self, topic: str, routing_key: str | None = None) -> Event:
        """Send a GET request to /events/{topic}/last."""
        params = QueryParams(routing_key=routing_key)
        return await self._get(f"/events/{topic}/last", Event, params=params)
    
    async def get_n_events(
        self,
        topic: str,
        routing_key: str | None = None,
        n: int | None = None
    ) -> EventQueryResult:
        """Send a GET request to /events/{topic}."""
        params=QueryParams(routing_key=routing_key, n=n)
        return await self._get(f"/events/{topic}", EventQueryResult, params=params)
    
    async def stream_events(self, *topics: TopicSubscription) -> AsyncGenerator[Event, None]:
        """Send a GET request to /events/stream/{token} and stream events."""
        token = await self.subscribe_to_topics(*topics)
        path = f"/events/stream/{token.token}"
        async for event in self._sse("GET", path, Event):
            yield event

    async def create_unitop(self, unitop: UnitOp) -> Status:
        """Send a POST request to /unitop/save."""
        data = unitop.json()
        return await self._post("/unitop/save", Status, data)
    
    async def get_unitop(self, unitop: str) -> Topic:
        """Send a GET request to /unitop/search/{unitop_id}."""
        return await self._get(f"/unitop/search/{unitop}", UnitOp)
    
    async def get_unitops(self, q: Dict[str, Any]) -> UnitOpQueryResult:
        """Set a GET request to /events/topics."""
        params = QueryParams(q=json.dumps(q))
        return await self._get("/events/topics", UnitOpQueryResult, params=params)
    
    async def publish_samples(self, samples: TimeseriesSamples) -> Status:
        """Send a POST request to /unitop/samples/publish."""
        data = samples.json()
        return await self._post("/unitop/samples/publish", Status, data)
    
    async def stream_unitop_data(self, unitop: str) -> AsyncGenerator[AnySubscriberMessage, None]:
        """Send a GET request to /unitop/stream/{unitop} and stream data."""
        path = f"/unitop/stream/{unitop}"
        async for event in self._sse("GET", path, AnySubscriberMessage):
            yield event

    async def download_unitop_data_to_csv(
        self,
        unitop: str,
        filepath: os.PathLike,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        scan_rate: int = 5
    ) -> None:
        """Send a GET request to /unitop/samples/{unitop} and download the file
        to CSV.
        """
        filepath = pathlib.Path(filepath)
        if filepath.suffix.lower != ".csv":
            raise ValueError("'filepath' suffix must be '.csv'")
        path = f"/unitop/samples/{unitop}"
        params = QueryParams(start_time=start_time, end_time=end_time, scan_rate=scan_rate)
        await self._download_to_csv(
            filepath,
            "GET",
            path,
            params=params
        )

    async def _get(
        self,
        path: str,
        response_model: Type[BaseModel],
        params: QueryParams | None = None
    ) -> BaseModel:
        """Handle GET request and return the response model."""
        response = await self._client.get(path, params=params)
        content = await response.aread()
        return response_model.parse_raw(content)
    
    async def _post(
        self,
        path: str,
        response_model: Type[BaseModel],
        json: Any,
        params: QueryParams | None = None,
    ) -> BaseModel:
        """Handle POST request and return the response model."""
        response = await self._client.post(path, params=params, json=json)
        content = await response.aread()
        return response_model.parse_raw(content)
    
    async def _sse(
        self,
        method: str,
        path: str,
        data_model: Type[BaseModel],
        params: QueryParams | None = None,
        json: Any | None = None
    ) -> AsyncGenerator[BaseModel, None]:
        """Handle SSE endpoints yielding events as the data model."""
        parser = SSEParser(_LOGGER)
        async with self._client.stream(method, path, params=params, json=json) as response:
            response.raise_for_status()
            async for data in response.iter_bytes():
                parser.feed(data)
                for event in parser.events():
                    if event.event == "data":
                        try:
                            yield data_model.parse_raw(event.data)
                        except ValidationError:
                            _LOGGER.warning(
                                "Unable to parse sse",
                                extra={
                                    "model": data_model.__name__,
                                    "data": event.data
                                }
                            )

    async def _download_to_csv(
        self,
        filepath: os.PathLike,
        method: str,
        path: str,
        params: QueryParams | None = None,
        json: Any | None = None
    ) -> None:
        """Download data to a CSV file."""
        async with self._client.stream(
            method,
            path,
            params=params,
            json=json,
            headers={"Accept": "text/csv"}
        ) as response:
            response.raise_for_status()
            with tempfile.SpooledTemporaryFile(max_size=10_485_760, mode="w+") as tfh:
                async for line in response.aiter_lines():
                    tfh.write(line)
                else:
                    tfh.seek(0)
                    with open(filepath, mode='w') as fh:
                        while True:
                            fh.write(tfh.read(10_240))

    async def _download_to_pandas(
        self,
        method: str,
        path: str,
        params: QueryParams | None = None,
        json: Any | None = None
    ) -> None:
        """Download CSV data to pandas dataframe."""
        async with self._client.stream(
            method,
            path,
            params=params,
            json=json,
            headers={"Accept": "text/csv"}
        ) as response:
            response.raise_for_status()
            with tempfile.SpooledTemporaryFile(max_size=10_485_760, mode="w+") as tfh:
                async for line in response.aiter_lines():
                    tfh.write(line)
                else:
                    tfh.seek(0)
                    return pd.read_csv(tfh)

    async def __aenter__(self):
        """Start the client.
        
        If the client is already started, this will raise an exception.
        
        If the client is already closed, this will raise an exception. Use a new client
        instance instead.
        """
        if self._closed:
            raise RuntimeError(
                "The client cannot be started again after closing. "
                "Retrieve a new client with `get_client()` instead."
            )

        if self._started:
            raise RuntimeError("The client cannot be started more than once.")

        await self._exit_stack.__aenter__()
        _LOGGER.debug("Connecting to API at %s", self.api_url)
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