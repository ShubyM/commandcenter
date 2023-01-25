import asyncio
import logging
from collections import deque
from collections.abc import AsyncIterable, Sequence
from contextlib import suppress
from types import TracebackType
from typing import Any, Deque, Dict, List, Set, Type

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
        self.connections: Dict[asyncio.Future, Connection] = {}
        self.data_queue: asyncio.Queue = asyncio.Queue(maxsize=max_buffered_messages)
        self.dropped_queue: asyncio.Queue = asyncio.Queue()
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    @property
    def capacity(self) -> int:
        raise NotImplementedError()

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        subscriptions = set()
        for fut, connection in self.connections.items():
            if not fut.done():
                subscriptions.update(connection.subscriptions)
        return subscriptions

    async def close(self) -> None:
        raise NotImplementedError()

    async def messages(self) -> AsyncIterable[str]:
        while not self.closed:
            yield await self.data_queue.get()
        else:
            raise ClientClosed()

    async def dropped(self) -> AsyncIterable[DroppedConnection]:
        while not self.closed:
            yield await self.dropped_queue.get()
        else:
            raise ClientClosed()

    def subscribe(self, subscriptions: Set[BaseSubscription]) -> None:
        raise NotImplementedError()

    def unsubscribe(self, subscriptions: Sequence[BaseSubscription]) -> None:
        raise NotImplementedError()

    def connection_lost(self, fut: asyncio.Future) -> None:
        assert fut in self.connections
        connection = self.connections.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        msg = DroppedConnection(subscriptions=connection.subscriptions, error=e)
        self.dropped_queue.put_nowait(msg)

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
        self.subscriptions: Set[BaseSubscription] = set()
        self.data_queue: asyncio.Queue = None
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    async def run(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError()

    async def start(
        self,
        subscriptions: Set[BaseSubscription],
        data_queue: asyncio.Queue,
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
        self.client = client
        self.subscriber = subscriber
        self.max_subscribers = max_subscribers
        self.maxlen = maxlen

        self.closed: bool = False
        self.subscribers: Dict[asyncio.Task, Subscriber] = {}
        self.errors: List[Exception] = []
        self.subscriber_event: asyncio.Event = asyncio.Event()
        self.ready: asyncio.Event = asyncio.Event()
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        subscriptions = set()
        for fut, subscriber in self.subscribers.items():
            if not fut.done():
                subscriptions.update(subscriber.subscriptions)
        return subscriptions

    async def close(self) -> None:
        self.closed = True
        for fut in self.subscribers.keys():
            fut.cancel()
        await self.client.close()

    async def subscribe(self, subscriptions: Sequence[BaseSubscription]) -> "Subscriber":
        raise NotImplementedError()

    def subscriber_lost(self, fut: asyncio.Future) -> None:
        assert fut in self.subscribers
        self.subscribers.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        if e is not None:
            _LOGGER.warning("Unhandled error in subscriber", exc_info=e)

    def __del__(self):
        try:
            if not self.closed:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(self.close())
        except Exception:
            pass


class BaseSubscriber(Subscriber):
    """Base implementation for a subscriber.
    
    Args:
        maxlen: The number of messages that can be buffered on the subscriber.
            If the buffer exceeds `maxlen` the oldest messages are evicted.
    """
    def __init__(self) -> None:
        self.subscriptions = set()
        self.data_queue: Deque[str] = None
        self.data_waiter: asyncio.Future = None
        self.stop_waiter: asyncio.Future = None
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    def stop(self, e: Exception | None) -> None:
        waiter = self.stop_waiter
        self.stop_waiter = None
        if waiter is not None and not waiter.done():
            if e is not None:
                waiter.set_exception(e)
            else:
                waiter.set_result(None)

    def publish(self, data: str) -> None:
        self.data_queue.append(data)
        waiter = self.data_waiter
        self.data_waiter = None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
    
    async def start(self, subscriptions: Set[BaseSubscription], maxlen: int) -> None:
        assert self.stop_waiter is None
        self.subscriptions.update(subscriptions)
        waiter = self.loop.create_future()
        self.stop_waiter = waiter

        self.data_queue = deque(maxlen=maxlen)

        await waiter

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