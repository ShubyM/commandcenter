import asyncio
import logging
from collections import deque
from collections.abc import AsyncIterable, Sequence
from contextlib import suppress
from enum import IntEnum
from types import TracebackType
from typing import Any, Deque, Dict, Set, Type

from commandcenter.exceptions import ClientClosed
from commandcenter.integrations.models import BaseSubscription, DroppedConnection
from commandcenter.integrations.protocols import (
    Client,
    Connection,
    Manager,
    Subscriber
)



_LOGGER = logging.getLogger("commandcenter.integrations")


class BaseClient(Client):
    """Base implementation for a client.
    
    Args:
        max_buffered_messages: The max length of the data queue for the client.
    """
    def __init__(self, max_buffered_messages: int = 1000) -> None:
        self._connections: Dict[asyncio.Future, Connection] = {}
        self._data: asyncio.Queue = asyncio.Queue(maxsize=max_buffered_messages)
        self._dropped: asyncio.Queue = asyncio.Queue()
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    @property
    def capacity(self) -> int:
        raise NotImplementedError()

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        subscriptions = set()
        for fut, connection in self._connections.items():
            if not fut.done():
                subscriptions.update(connection.subscriptions)
        return subscriptions

    async def close(self) -> None:
        raise NotImplementedError()

    async def messages(self) -> AsyncIterable[str]:
        while not self.closed:
            msg = await self._data.get()
            if self.closed:
                raise ClientClosed()
            yield msg
        else:
            raise ClientClosed()

    async def dropped(self) -> AsyncIterable[DroppedConnection]:
        while not self.closed:
            msg = await self._dropped.get()
            if self.closed:
                raise ClientClosed()
            yield msg
        else:
            raise ClientClosed()

    def subscribe(self, subscriptions: Set[BaseSubscription]) -> None:
        raise NotImplementedError()

    def unsubscribe(self, subscriptions: Sequence[BaseSubscription]) -> None:
        raise NotImplementedError()

    def connection_lost(self, fut: asyncio.Future) -> None:
        assert fut in self._connections
        connection = self._connections.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        # If a connection was cancelled by the client and the subscriptions were
        # replaced through another connection, subscriptions will be empty set
        msg = DroppedConnection(
            subscriptions=connection.subscriptions.difference(self.subscriptions),
            error=e
        )
        self._dropped.put_nowait(msg)

    def __del__(self):
        try:
            if not self.closed:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(self.close())
        except Exception:
            pass


class BaseConnection(Connection):
    """Base implementation for a connection."""
    def __init__(self) -> None:
        self._subscriptions: Set[BaseSubscription] = set()
        self._data: asyncio.Queue = None
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        return self._subscriptions

    async def run(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError()

    async def start(
        self,
        subscriptions: Set[BaseSubscription],
        data: asyncio.Queue,
        *args: Any,
        **kwargs: Any
    ) -> None:
        raise NotImplementedError()


class BaseManager(Manager):
    """Base implementation for a manager.

    Args:
        client: The client instance which connects to and streams data from the
            data source.
        subscriber: The subscriber type to use for this manager.
        max_subscribers: The maximum number of concurrent subscribers which can
            run by a single manager. If the limit is reached, the manager will
            refuse the attempt and raise a `CapacityError`.
        maxlen: The maximum number of messages that can buffered on the subscriber.
            If the buffer limit on the subscriber is reached, the oldest messages
            will be evicted as new messages are added.
    """
    def __init__(
        self,
        client: Client,
        subscriber: Type[Subscriber],
        max_subscribers: int = 100,
        maxlen: int = 100
    ) -> None:
        self._client = client
        self._subscriber = subscriber
        self._max_subscribers = max_subscribers
        self._maxlen = maxlen

        self._closed: bool = False
        self._subscribers: Dict[asyncio.Task, Subscriber] = {}
        self._event: asyncio.Event = asyncio.Event()
        self._ready: asyncio.Event = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        subscriptions = set()
        for fut, subscriber in self._subscribers.items():
            if not fut.done():
                subscriptions.update(subscriber.subscriptions)
        return subscriptions

    async def close(self) -> None:
        self.closed = True
        for fut in self._subscribers.keys():
            fut.cancel()
        await self._client.close()

    async def subscribe(self, subscriptions: Sequence[BaseSubscription]) -> "Subscriber":
        raise NotImplementedError()

    def subscriber_lost(self, fut: asyncio.Future) -> None:
        assert fut in self._subscribers
        subscriber = self._subscribers.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        if e is not None:
            _LOGGER.warning("Unhandled error in %s", subscriber.__class__.__name__, exc_info=e)

    def __del__(self):
        try:
            if not self.closed:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(self.close())
        except Exception:
            pass


class SubscriberCodes(IntEnum):
    STOPPED = 1
    DATA = 2


class BaseSubscriber(Subscriber):
    """Base implementation for a subscriber.
    
    Args:
        maxlen: The number of messages that can be buffered on the subscriber.
            If the buffer exceeds `maxlen` the oldest messages are evicted.
    """
    def __init__(self) -> None:
        self._subscriptions = set()
        self._data: Deque[str] = None
        self._data_waiter: asyncio.Future = None
        self._stop_waiter: asyncio.Future = None
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    @property
    def stopped(self) -> bool:
        return self._stop_waiter is not None and not self._stop_waiter.done()

    def stop(self, e: Exception | None) -> None:
        waiter = self._stop_waiter
        self._stop_waiter = None
        if waiter is not None and not waiter.done():
            if e is not None:
                waiter.set_exception(e)
            else:
                waiter.set_result(None)
        _LOGGER.debug("%s stopped", self.__class__.__name__)

    def publish(self, data: str) -> None:
        self._data.append(data)
        waiter = self._data_waiter
        self._data_waiter = None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        _LOGGER.debug("Message published to %s", self.__class__.__name__)
    
    async def start(self, subscriptions: Set[BaseSubscription], maxlen: int) -> None:
        assert self._stop_waiter is None
        self.subscriptions.update(subscriptions)
        waiter = self._loop.create_future()
        self._stop_waiter = waiter

        self._data = deque(maxlen=maxlen)

        await waiter

    async def wait_for_data(self) -> None:
        stop = self._stop_waiter
        if stop is None or stop.done():
            return SubscriberCodes.STOPPED
        waiter = self._loop.create_future()
        self._data_waiter = waiter
        await asyncio.wait([waiter, stop], return_when=asyncio.FIRST_COMPLETED)
        if not waiter.done(): # Stop called
            _LOGGER.debug("%s stopped waiting for data", self.__class__.__name__)
            waiter.cancel()
            self._data_waiter = None
            return SubscriberCodes.STOPPED
        return SubscriberCodes.DATA

    def __enter__(self) -> "Subscriber":
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None
    ) -> None:
        if isinstance(exc_value, Exception): # Not CancelledError
            self.stop(exc_value)
        else:
            self.stop(None)