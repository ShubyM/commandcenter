import asyncio
import itertools
import logging
from collections import deque
from collections.abc import AsyncIterable, Sequence
from contextlib import suppress
from datetime import timedelta
from types import TracebackType
from typing import Any, Deque, Dict, List, Set, Type

from commandcenter.integrations.exceptions import ClientClosed
from commandcenter.integrations.models import BaseSubscription, ErrorMessage
from commandcenter.integrations.protocols import (
    Client,
    Collection,
    Connection,
    Manager,
    Subscriber
)


_LOGGER = logging.getLogger("commandcenter.integrations")


class BaseClient(Client):
    """Base implementation for all real time integration clients.
    
    A client manages a pool of connections to a data source, the underlying
    protocols may vary.
    
    Clients implement the interface bridging the gap between a manager the protocol
    required to connect to a data source. Clients should rarely (if ever) be
    handled outside the scope of a manager.
    
    Args:
        max_buffered_messages: The max length of the data queue for the client.
    """
    def __init__(self, max_buffered_messages: int = 1000) -> None:
        self.connections: Dict[asyncio.Future, Connection] = {}
        self.data_queue: asyncio.Queue = asyncio.Queue(maxsize=max_buffered_messages)
        self.errors_queue: asyncio.Queue = asyncio.Queue()
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    @property
    def capacity(self) -> int:
        """The current capacity of the client. Clients can handle a finite
        number of subscriptions.
        """
        raise NotImplementedError()

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        """The current subscriptions supported by the client."""
        subscriptions = set()
        for fut, connection in self.connections.items():
            if not fut.done():
                subscriptions.update(connection.subscribtions)
        return subscriptions

    async def messages(self) -> AsyncIterable[str]:
        """Receive incoming messages from all connections.
        
        This is the central point for data flow through the client to the manager.
        """
        while not self.closed:
            yield await self.data_queue.get()
        else:
            raise ClientClosed()

    async def errors(self) -> AsyncIterable[ErrorMessage]:
        """Receive errors from connections.
        
        Managers receive errors from clients and drop any subscribers dependent
        on the connection's subscriptions.
        """
        while not self.closed:
            yield await self.errors_queue.get()
        else:
            raise ClientClosed()

    async def subscribe(self, subscriptions: Sequence[BaseSubscription]) -> bool:
        """Start the appropriate number of connections to support all subscriptions.
        
        If the client is capped and cannot support all subscriptions this method
        returns `False` meaning no connections were opened.

        Args:
            subscriptions: The subscriptions to subscribe to.

        Returns:
            status: A boolean indicating whether or not the operation was
                successful. If `True`, *all* subscriptions were successfully
                subscribed, if `False` *none* of the subscriptions were subscribed
                to.
        """
        raise NotImplementedError()

    async def unsubscribe(self, subscriptions: Sequence[BaseSubscription]) -> bool:
        """Close connections supporting the subscriptions.
        
        This method should stop the appropriate connections and clean up any
        resources as needed. However, it *should* not interrupt service (i.e stop
        connections in use) for any other subscriptions already supported by
        the client.
        
        Args:
            subscriptions: The subscriptions to unsubscribe from.

        Returns:
            status: A boolean indicating whether or not the operation was
                successful. If `True`, all subscriptions were unsubscribed
                from, if `False` *none* of the subscriptions were unsubscribed
                from.
        """

    def connection_lost(self, fut: asyncio.Future) -> None:
        assert fut in self.connections
        connection = self.connections.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        if e is not None:
            msg = ErrorMessage(error=e, subscriptions=connection.subscriptions)
            self.errors_queue.put_nowait(msg)

    def __del__(self):
        try:
            if not self.closed:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(self.close())
        except Exception:
            pass


class BaseConnection(Connection):
    """Base implementation for connections."""
    def __init__(self) -> None:
        self.subscriptions: Set[BaseSubscription] = set()
        self.data_queue: asyncio.Queue = None
        self.online: bool = False
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    def toggle(self) -> None:
        self.online = not self.online

    async def start(
        self,
        subscriptions: Set[BaseSubscription],
        data_queue: asyncio.Queue,
        *args: Any,
        **kwargs: Any
    ) -> None:
        self.subscriptions.update(subscriptions)
        self.data_queue = data_queue
        await self.run(*args, **kwargs)


class BaseManager(Manager):
    """Base manager impelementation.

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
        self.background: Set[asyncio.Task] = set()
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
        for fut in itertools.chain(
            self.subscribers.keys(),
            self.background
        ):
            fut.cancel()
        await self.client.close()

    def subscriber_lost(self, fut: asyncio.Future) -> None:
        assert fut in self.subscribers
        self.subscribers.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        if e is not None:
            _LOGGER.warning("Error in subscriber", exc_info=e)

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


class BaseCollection(Collection):
    def __init__(
        self,
        subscriptions: Set[BaseSubscription],
        delta: timedelta | float
    ) -> None:
        self.subscriptions = subscriptions
        if isinstance(delta, int):
            delta = timedelta(seconds=delta)
        if not isinstance(delta, timedelta):
            raise TypeError(
                f"Invalid type for 'delta', expected timedelta | int, got {type(delta)}"
            )
        self.delta = delta

        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.close_waiter: asyncio.Future = loop.create_future()

    @property
    def closed(self) -> bool:
        return self.close_waiter is not None and not self.close_waiter.done()

    async def close(self) -> None:
        waiter = self.close_waiter
        self.close_waiter = None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)