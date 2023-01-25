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
    for subscription in subscriptions:
        if subscription.web_id_type != web_id_type:
            raise ValueError(f"Invalid WebIdType for subscription {subscription.web_id}")
    
    return (
        "/piwebapi/streamsets/channel?webId="
        f"{'&webId='.join([subscription.web_id for subscription in subscriptions])}"
        f"&webIdType={web_id_type}"
    )


async def connect(
    session: ClientSession,
    url: str,
    protocols: Optional[Iterable[str]],
    heartbeat: float,
    close_timeout: float,
    max_msg_size: int,
) -> ClientWebSocketResponse:
    return await session.ws_connect(
        url,
        protocols=protocols,
        heartbeat=heartbeat,
        timeout=close_timeout,
        max_msg_size=max_msg_size
    )


class PIConnection(BaseConnection):
    """Represents a single websocket connection to a PI Web API '/streamset'
    endpoint.
    """
    def __init__(self, max_subscriptions: int) -> None:
        super().__init__()
        self.max_subscriptions = max_subscriptions

    @property
    def capacity(self) -> int:
        """The number of additional subscriptions this connection can support."""
        return self.max_subscriptions - len(self.subscriptions)

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
        
        url = build_url(web_id_type=web_id_type, subscriptions=self.subscriptions)
        ws = await connect(
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
                        await self.data_queue.put(data.json())
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
                                ws = await connect(
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
        data_queue: asyncio.Queue,
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
        self.subscriptions.update(subscriptions)
        self.data_queue = data_queue
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


class PIClient(BaseClient):
    """Client implementation for real-time data from the PI Web API."""
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
            ["PIConnection", Set[PISubscription], asyncio.Queue],
            Awaitable[None]
        ]= functools.partial(
            PIConnection.start,
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
        task = self._consolidation
        self._consolidation = None
        if task is not None:
            task.cancel()
        for fut, _ in self.connections.items(): fut.cancel()
        await self._session.close()

    def subscribe(self, subscriptions: Set[PISubscription]) -> None:
        if self.closed:
            raise ClientClosed()

        existing = self.subscriptions
        capacity = self.capacity
        subscriptions = list(subscriptions.difference(existing))
        count = len(subscriptions)
        
        if subscriptions and capacity > count:
            connections = self._create_connections(subscriptions)
            self.connections.update(connections)
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
            to_cancel: Dict[asyncio.Task, PIConnection] = {}
            for fut, connection in self.connections.items():
                if (
                    len(connection.subscriptions.difference(subscriptions)) !=
                    len(connection.subscriptions)
                ):
                    to_cancel[fut] = connection
                    to_keep.extend(connection.subscriptions.difference(subscriptions))
            
            if to_keep:
                connections = self._create_connections(to_keep)
                self.connections.update(connections)
            
            for fut in to_cancel.keys():
                fut.cancel()
            
            self._event.set()
            _LOGGER.debug("Unsubscribed from %i subscriptions", len(subscriptions))

    def _create_connections(self, subscriptions: List[str]) -> Dict[asyncio.Task, PIConnection]:
        connections: Dict[asyncio.Task, PIConnection] = {}
        
        while True:
            if len(subscriptions) <= self._max_subscriptions:
                # This connection will cover the remaining
                connection = PIConnection(max_subscriptions=self._max_subscriptions)
                fut = self.loop.create_task(
                    self._start_connection(
                        connection,
                        set(subscriptions),
                        self.data_queue
                    )
                )
                connections[fut] = connection
                break
            
            else: # More connections are required
                connection = PIConnection(max_subscriptions=self._max_subscriptions)
                fut = self.loop.create_task(
                    self._start_connection(
                        connection,
                        set(subscriptions[:self._max_subscriptions]),
                        self.data_queue
                    )
                )
                connections[fut] = connection
                del subscriptions[:self._max_subscriptions]

        return connections

    async def _run_consolidation(self) -> None:
        while True:
            await self._event.wait()
            await asyncio.sleep(5)

            try:
                _LOGGER.debug("Scanning connections for consolidation")
                has_capacity = {
                    fut: connection for fut, connection in self.connections.items()
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
                self.connections.update(connections)

                for fut in has_capacity.keys():
                    fut.cancel()
                
                _LOGGER.debug(
                    "Consolidated %i connections",
                    len(has_capacity)-len(connections)
                )
            
            finally:
                self._event.clear()

    def _start(self, _: asyncio.Future | None) -> None:
        if self.closed or self._close_called:
            return
        self._consolidation = None
        t = self.loop.create_task(self._run_consolidation())
        t.add_done_callback(self._start)
        self._consolidation = t