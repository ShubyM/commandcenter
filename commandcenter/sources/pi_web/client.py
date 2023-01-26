import asyncio
import functools
import logging
import math
from collections.abc import Awaitable, Iterable
from typing import (
    Callable,
    Dict,
    List,
    Optional,
    Set
)

from aiohttp import ClientSession, ClientWebSocketResponse
from pendulum.tz.zoneinfo.exceptions import InvalidTimezone
from pydantic import ValidationError

from commandcenter.exceptions import ClientClosed, SubscriptionLimitError
from commandcenter.integrations.base import BaseClient, BaseConnection
from commandcenter.sources.pi_web.models import (
    PIMessage,
    PISubscription,
    WebIdType
)
from commandcenter.util import TIMEZONE, EqualJitterBackoff



_LOGGER = logging.getLogger("commandcenter.sources.pi_web")


def build_url(web_id_type: WebIdType, subscriptions: Set[PISubscription]) -> str:
    """Build websocket '/streamset' url."""
    for subscription in subscriptions:
        if subscription.web_id_type != web_id_type:
            raise ValueError(f"Invalid WebIdType for subscription {subscription.web_id}")
    
    return (
        "/piwebapi/streamsets/channel?webId="
        f"{'&webId='.join([subscription.web_id for subscription in subscriptions])}"
        f"&webIdType={web_id_type}"
    )


async def create_connection(
    session: ClientSession,
    url: str,
    protocols: Optional[Iterable[str]],
    heartbeat: float,
    close_timeout: float,
    max_msg_size: int,
) -> ClientWebSocketResponse:
    """Open websocket connection to PI Web API."""
    return await session.ws_connect(
        url,
        protocols=protocols,
        heartbeat=heartbeat,
        timeout=close_timeout,
        max_msg_size=max_msg_size
    )


class PIWebConnection(BaseConnection):
    """Represents a single websocket connection to a PI Web API '/streamset'
    endpoint.
    """
    def __init__(self, max_subscriptions: int) -> None:
        super().__init__()
        self._max_subscriptions = max_subscriptions

    @property
    def capacity(self) -> int:
        """The number of additional subscriptions this connection can support."""
        return self._max_subscriptions - len(self._subscriptions)

    async def run(
        self,
        session: ClientSession,
        web_id_type: WebIdType,
        max_reconnect_attempts: int,
        initial_backoff: float,
        max_backoff: float,
        protocols: Optional[Iterable[str]],
        heartbeat: float,
        close_timeout: float,
        max_msg_size: int,
        timezone: str
    ) -> None:
        """Open a websocket connection to the PI Web API and process data
        indefinitely.
        """
        url = build_url(web_id_type=web_id_type, subscriptions=self._subscriptions)
        ws = await create_connection(
            session=session,
            url=url,
            protocols=protocols,
            heartbeat=heartbeat,
            close_timeout=close_timeout,
            max_msg_size=max_msg_size
        )
        backoff = EqualJitterBackoff(max=max_backoff, initial=initial_backoff)

        try:
            while True:
                async for msg in ws:
                    try:
                        data = PIMessage.parse_raw(msg.data)
                    except ValidationError:
                        _LOGGER.warning(
                            "Message validation failed",
                            exc_info=True,
                            extra={"raw": msg.data}
                        )
                    except Exception:
                        _LOGGER.error(
                            "An unhandled error occurred parsing the message",
                            exc_info=True,
                            extra={"raw": msg.data}
                        )
                    else:
                        try:
                            data.in_timezone(timezone)
                        except InvalidTimezone:
                            raise
                        await self._data.put(data.json())
                else:
                    assert ws.closed
                    close_code = ws.close_code
                    _LOGGER.warning(
                        "Websocket closed by peer or network failure %i",
                        close_code
                    )
                    e = ws.exception()
                    if max_reconnect_attempts is not None and max_reconnect_attempts > 0:
                        attempts = 0
                        while attempts < max_reconnect_attempts:
                            _LOGGER.info(
                                "Attempting reconnect. Attempt %i of %i",
                                attempts + 1,
                                max_reconnect_attempts
                            )
                            try:
                                ws = await create_connection(
                                    session=session,
                                    url=url,
                                    protocols=protocols,
                                    heartbeat=heartbeat,
                                    close_timeout=close_timeout,
                                    max_msg_size=max_msg_size
                                )
                            except Exception:
                                backoff_delay = backoff.compute(attempts)
                                _LOGGER.debug(
                                    "Reconnect failed. Trying again in %0.2f seconds",
                                    backoff_delay,
                                    exc_info=True
                                )
                                await asyncio.sleep(backoff_delay)
                                attempts += 1
                            else:
                                _LOGGER.info("Connection re-established")
                                break
                        if ws.closed:
                            if e is not None:
                                raise e
                            break
                    else:
                        if e is not None:
                            raise e
                        break
        finally:
            if not ws.closed:
                await ws.close()
    
    async def start(
        self,
        subscriptions: Set[PISubscription],
        data: asyncio.Queue,
        session: ClientSession,
        web_id_type: WebIdType,
        max_reconnect_attempts: int,
        initial_backoff: float,
        max_backoff: float,
        protocols: Optional[Iterable[str]],
        heartbeat: float,
        close_timeout: float,
        max_msg_size: int,
        timezone: str
    ) -> None:
        self._subscriptions.update(subscriptions)
        self._data = data
        await self.run(
            session=session,
            web_id_type=web_id_type,
            max_reconnect_attempts=max_reconnect_attempts,
            initial_backoff=initial_backoff,
            max_backoff=max_backoff,
            protocols=protocols,
            heartbeat=heartbeat,
            close_timeout=close_timeout,
            max_msg_size=max_msg_size,
            timezone=timezone
        )


class PIWebClient(BaseClient):
    """Client implementation for real-time streaming from the PI Web API."""
    def __init__(
        self,
        session: ClientSession,
        web_id_type: WebIdType = WebIdType.FULL,
        *,
        max_connections: int = 50,
        max_subscriptions: int = 30,
        max_buffered_messages: int = 1000,
        max_reconnect_attempts: int = 5,
        initial_backoff: float = 5.0,
        max_backoff: float = 60.0,
        protocols: Optional[Iterable[str]] = None,
        heartbeat: float = 20.0,
        close_timeout: float = 10.0,
        max_msg_size: int = 4*1024*1024,
        timezone: str = TIMEZONE
    ) -> None:
        super().__init__(max_buffered_messages)
        
        self._start_connection: Callable[
            [PIWebConnection, Set[PISubscription], asyncio.Queue],
            Awaitable[None]
        ]= functools.partial(
            PIWebConnection.start,
            session=session,
            web_id_type=web_id_type,
            max_reconnect_attempts=max_reconnect_attempts,
            initial_backoff=initial_backoff,
            max_backoff=max_backoff,
            protocols=protocols,
            heartbeat=heartbeat,
            close_timeout=close_timeout,
            max_msg_size=max_msg_size,
            timezone=timezone
        )

        self._session = session
        self._max_capacity = max_connections * max_subscriptions
        self._max_subscriptions = max_subscriptions
        
        self._consolidation: asyncio.Task = None
        self._event: asyncio.Event = asyncio.Event()
        self._close_called: bool = False

        self._start(None)
    
    @property
    def capacity(self) -> int:
        return self._max_capacity - len(self.subscriptions)

    @property
    def closed(self) -> bool:
        return self._session.closed

    async def close(self) -> None:
        self._close_called = True
        fut = self._consolidation
        self._consolidation = None
        if fut is not None:
            fut.cancel()
        for fut, _ in self._connections.items(): fut.cancel()
        await self._session.close()

    def subscribe(self, subscriptions: Set[PISubscription]) -> None:
        if self.closed:
            raise ClientClosed()

        subscriptions = list(subscriptions.difference(self.subscriptions))
        capacity = self.capacity
        count = len(subscriptions)
        
        if subscriptions and capacity > count:
            connections = self._create_connections(subscriptions)
            self._connections.update(connections)
            self._event.set()
            _LOGGER.debug("Subscribed to %i subscriptions", count)
        else:
            raise SubscriptionLimitError(self._max_capacity)

    def unsubscribe(self, subscriptions: Set[PISubscription]) -> bool:
        if self.closed:
            raise ClientClosed()

        not_applicable = subscriptions.difference(self.subscriptions)
        subscriptions = subscriptions.difference(not_applicable)
        
        if subscriptions:
            # Determine the subscriptions we need to keep from existing
            # connections
            to_keep: List[PISubscription] = []
            to_cancel: Dict[asyncio.Task, PIWebConnection] = {}
            for fut, connection in self._connections.items():
                if (
                    len(connection.subscriptions.difference(subscriptions)) !=
                    len(connection.subscriptions)
                ):
                    to_cancel[fut] = connection
                    to_keep.extend(connection.subscriptions.difference(subscriptions))
            
            if to_keep:
                connections = self._create_connections(to_keep)
                self._connections.update(connections)
            
            for fut in to_cancel.keys():
                fut.cancel()
            
            self._event.set()
            _LOGGER.debug("Unsubscribed from %i subscriptions", len(subscriptions))

    def _create_connections(self, subscriptions: List[str]) -> Dict[asyncio.Task, PIWebConnection]:
        """Creates the appropriate number of connections to support all
        subscriptions.
        
        You can stream multiple PI tags on a single websocket connection. You are
        only limited by the length of the URL.
        """
        connections: Dict[asyncio.Task, PIWebConnection] = {}
        
        while True:
            if len(subscriptions) <= self._max_subscriptions:
                # This connection will cover the remaining
                connection = PIWebConnection(max_subscriptions=self._max_subscriptions)
                fut = self._loop.create_task(
                    self._start_connection(
                        connection,
                        set(subscriptions),
                        self._data
                    )
                )
                connections[fut] = connection
                break
            
            else: # More connections are required
                connection = PIWebConnection(max_subscriptions=self._max_subscriptions)
                fut = self._loop.create_task(
                    self._start_connection(
                        connection,
                        set(subscriptions[:self._max_subscriptions]),
                        self._data
                    )
                )
                connections[fut] = connection
                del subscriptions[:self._max_subscriptions]

        return connections

    async def _run_consolidation(self) -> None:
        """Ensures the optimal number of websocket connections are open.
        
        `subscribe` is a purely additive operation while `unsubscribe` can leave
        connections without an optimal number of connections. This class closes
        connections and reopens them with the maximum number of subscriptions
        to optimize connection usage.
        """
        while True:
            await self._event.wait()
            await asyncio.sleep(5)

            try:
                _LOGGER.debug("Scanning connections for consolidation")
                has_capacity = {
                    fut: connection for fut, connection in self._connections.items()
                    if len(connection.subscriptions) < self._max_subscriptions
                }
                capacity = sum(
                    [
                        len(connection.subscriptions) for connection
                        in has_capacity.values()
                    ]
                )
                optimal = math.ceil(capacity/self._max_subscriptions)
                
                if optimal == len(has_capacity):
                    _LOGGER.debug("Optimal number of active connection")
                    continue

                _LOGGER.debug("Consolidating connections")
                
                subscriptions = []
                for connection in has_capacity.values():
                    subscriptions.extend(connection.subscriptions)
                
                connections = self._create_connections(subscriptions)
                self._connections.update(connections)

                for fut in has_capacity.keys():
                    fut.cancel()
                
                _LOGGER.debug(
                    "Consolidated %i connections",
                    len(has_capacity)-len(connections)
                )
            
            finally:
                self._event.clear()

    def _start(self, _: asyncio.Future | None) -> None:
        """Ensures the cosolidation background task is always running."""
        if self.closed or self._close_called:
            return
        self._consolidation = None
        fut = self._loop.create_task(self._run_consolidation())
        fut.add_done_callback(self._start)
        self._consolidation = fut