import asyncio
import uuid
from abc import ABC, abstractmethod, abstractproperty
from collections import deque
from collections.abc import AsyncIterable
from contextlib import suppress
from datetime import timedelta
from types import TracebackType
from typing import (
    Any,
    AsyncIterable,
    Callable,
    Deque,
    List,
    Optional,
    Sequence,
    Set,
    Type,
    Union,
)

from commandcenter.core.integrations.models import BaseSubscription, ErrorMessage
from commandcenter.core.integrations.types import TimeseriesRow



class AbstractClient(ABC):
    """Standard interface for all real-time client instances.
    
    A client manages a pool of connections to a data source. It is a subscription
    interface only, no I/O to the source actually occurrs in the client.
    
    Managers use client instances to ferry data from the source to a subscriber
    interested in a particular set of subscriptions. Clients should rarely
    (if ever) be handled outside the context of a manager.

    Args:
        max_buffered_messages: The max length of the data queue for the client.
    """

    def __init__(self, max_buffered_messages: int = 1000) -> None:
        self.connections: List["AbstractConnection"] = []
        self.errors_queue: asyncio.Queue = asyncio.Queue()
        self.data_queue: asyncio.Queue = asyncio.Queue(maxsize=max_buffered_messages)
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    
    @abstractproperty
    def capacity(self) -> int:
        """Return an integer indicating how many more subscriptions this client
        can support.
        """
        raise NotImplementedError()

    @abstractproperty
    def closed(self) -> bool:
        """Returns `True` if client is closed. A closed client has no connections
        and cannot accept subscriptions.
        """
        raise NotImplementedError()
    
    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        """Return a set of the subscriptions from all connections."""
        subscriptions = set()
        for connection in self.connections: subscriptions.update(connection.subscriptions)
        return subscriptions

    async def messages(self) -> AsyncIterable[str]:
        """Receive incoming messages from all connections.
        
        This is the central point for data flow through the client to the manager.
        The manager has no concept of the underlying workings of the client and
        its connection structure.
        """
        while True:
            yield await self.data_queue.get()

    async def errors(self) -> AsyncIterable[ErrorMessage]:
        """Receive errors that caused connections to fail.
        
        A failed connection equates to a loss of service which the manager
        must be notified about so that it can shut down any subscribers
        dependent on those subscriptions.
        """
        while True:
            yield await self.errors_queue.get()

    @abstractmethod
    async def close(self) -> None:
        """Close the client instance and shut down all connections.
        
        Any resources having to do with underlying I/O transport must be finalized
        here.
        """
        raise NotImplementedError()

    @abstractmethod
    async def subscribe(self, subscriptions: Sequence[BaseSubscription]) -> bool:
        """Subscribe to data points from a source.
        
        This method should create the appropriate number of connections to support
        all the subscriptions. Also, it *SHOULD* not interrupt service for any other
        subscriptions.

        Args:
            subscriptions: The subscriptions to subscribe to.

        Returns:
            status: A boolean indicating whether or not the operation was
                successful. If `True`, all subscriptions were successfully
                subscribed, if `False` *none* of the subscriptions were subscribed
                to.
        """
        raise NotImplementedError()

    @abstractmethod
    async def unsubscribe(self, subscriptions: Sequence[BaseSubscription]) -> bool:
        """Unsubscribe from data points from a source.
        
        This method should stop the appropriate connections and clean up any
        resources as needed. However, it *SHOULD* not interrupt service (i.e stop
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
        raise NotImplementedError()

    def connection_lost(self, connection: "AbstractConnection") -> None:
        """Callback for connection instances after they have stopped.
        
        If the connection stopped due to an unhandled exception, the exception
        will be packaged into an `ErrorMessage` and placed in the errors queue
        to be picked up by the manager.
        """
        exc = connection.exception
        if exc is not None:
            self.errors_queue.put_nowait(
                ErrorMessage(
                    exc=exc,
                    subscriptions=connection.subscriptions
                )
            )
        if connection in self.connections:
            self.connections.remove(connection)

    def __del__(self):
        try:
            if not self.closed:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(self.close())
        except Exception:
            pass


class AbstractConnection(ABC):
    """Standard interface for all real-time connection instances.
    
    A connection is where the actual I/O to a source occurs. A connection
    abstracts the protocol away from the parent client.
    
    Connections should never be initialized directly by an API. They should only
    be created, started, and stopped by an `AbstractClient`.
    
    Args:
        callback: The callback method on the client called when the connection
            stops.
    """
    def __init__(self, callback: Callable[["AbstractConnection"], None]) -> None:
        self.callback = callback
        
        self.exception: BaseException = None
        self.subscriptions: Set[BaseSubscription] = set()
        self.loop: asyncio.AbstractEventLoop = None
        self.feeder: asyncio.Queue = None
        self.stopped: bool = False
        self.online: bool = False
        self.runner: asyncio.Task = None
    
    @property
    def running(self) -> bool:
        """`True` if the runner task is not done."""
        runner = self.runner
        return runner is not None and not runner.done()
    
    def connection_lost(self, fut: asyncio.Future) -> None:
        """Callback method that *MUST* be added to the `runner` task when the
        connection is started.
        """
        exception = None
        with suppress(asyncio.CancelledError):
            exception = fut.exception()
        self.exception = exception
        try:
            self.loop.call_soon(self.callback, self)
        except RuntimeError:
            # Loop is closed
            pass

    def stop(self) -> None:
        """Stop the connection. This method is idempotent, multiple calls to
        `stop` will have no effect.
        """
        self.stopped = True
        runner = self.runner
        self.runner = None
        if runner is not None and not runner.done():
            runner.cancel()
    
    def toggle(self) -> None:
        """Toggle the status of the connection.
        
        If `online` is `True`, the connection can pass data to the client
        otherwise, the messages must be discarded.
        """
        if not self.running:
            raise RuntimeError("Attempted to toggle status of non-running connection")
        self.online = not self.online

    @abstractmethod
    async def run(self) -> None:
        """Perform I/O to data source in an infinite loop.
        
        This method is the coroutine for the `runner` task and should
        receive/retrieve, parse, and validate data from the source which it is
        connecting to.
        
        When the connection status is 'online' (i.e `online` is `True`) data
        may be passed to the client instance.
        
        This method must raise `asyncio.CancelledError` when cancelled. The exception
        may be caught in order to clean up resources but it must re-raised.
        """
        raise NotImplementedError()

    @abstractmethod
    async def start(
        self,
        subscriptions: Set[BaseSubscription],
        feeder: asyncio.Queue
    ) -> None:
        """Start the `runner` task.
        
        The `super` method *MUST* be called first in any implementation of this
        method. Before this method is called, the connection has no reference
        to the event loop.

        This method may perform any intial connection setup to the data source
        before starting the task.
        
        Any exceptions raised in the `start` method should not be suppressed
        they will be handled in the client.

        Args:
            subscriptions: A set of subscriptions to connect to at the datasource.
            feeder: The client data queue to place received messages into.
        """
        if self.running:
            raise RuntimeError("Attempted to start a running connection.")
        if self.stopped:
            raise RuntimeError("Connection cannot be started after stop was called.")
        if self.loop is None:
            self.loop = asyncio.get_running_loop()
        self.subscriptions.update(subscriptions)
        self.feeder = feeder

    def __del__(self) -> None:
        self.stop()


class AbstractManager(ABC):
    """Standard interface for all manager instances.
    
    A manager bridges the gap between a client instance, which retrieves data
    from a source, and a subscriber (the consumer of the data).
    
    Managers are designed to support any client instance which implements the
    `AbstractClient` interfaces. Managers are similar to fanout exchanges in
    RabbitMQ. In this case, a client is the publisher and the subscribers are
    the queues.

    The manager-subscriber model abstracts away all the protocol crap from
    data integrations. It is designed to be a backplane for consistent data
    structures and allow subscribers streaming from multiple sources to merge
    those streams in a trivial way.
    
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
        client: "AbstractClient",
        subscriber: Type["AbstractSubscriber"],
        max_subscribers: int = 100,
        maxlen: int = 100
    ) -> None:
        self.client = client
        self.subscriber = subscriber
        self.max_subscribers = max_subscribers
        self.maxlen = maxlen

        self.closed: bool = False
        self.subscribers: List[AbstractSubscriber] = []
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        """Return a set of the subscriptions from all subscribers."""
        subscriptions = set()
        for subscriber in self.subscribers: subscriptions.update(subscriber.subscriptions)
        return subscriptions

    async def close(self) -> None:
        """Stop all subscribers and close the client."""
        if not self.closed:
            for subscriber in self.subscribers: subscriber.stop()
            self.subscribers.clear()
            self.closed = True
            await self.client.close()

    @abstractmethod
    async def subscribe(
        self,
        subscriptions: Sequence[BaseSubscription]
    ) -> "AbstractSubscriber":
        """Subscribe to the subscriptions on the client and configure a subscriber.
        
        If the subscription process on the client fails, this *MUST* raise a
        `SubscriptionError`.

        This method *MUST* check the capacity of the manager to ensure it can
        support another subscriber. If it cannot, this *MUST* raise a `CapacityError`
        """
        raise NotImplementedError()

    @abstractmethod
    def subscriber_lost(self, subscriber: "AbstractSubscriber") -> None:
        """Callback for subscriber instances after their `stop` method was called.
        
        At a minumum, this *MUST* remove the subscriber from the list of subscribers
        for the manager. It *SHOULD* signal other tasks to check clients and see
        if their subscriptions are still required.
        """
        raise NotImplementedError()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}: Using {self.client.__class__.__name__} client and {self.subscriber.__name__} subscriber."

    def __del__(self):
        try:
            if not self.closed:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(self.close())
        except Exception:
            pass


class AbstractSubscriber(AsyncIterable[Union[str, bytes]]):
    """Standard interface for all subscriber instances.
    
    Subscribers stream data published to them from a manager. Subscribers are
    the final gatekeeper for data integrity and validation. They have two
    requirements.
    
    1. Subscribers *MUST* ensure timeseries data is in monotonically increasing
    order.
    2. Subscribers *MUST* ensure they only proxy subscriptions which they are
    responsible for. 
    
    Depending on the manager implementation, there is no guarentee that
    a message received in the subscribers queue is intended for this subscriber.
    Some manager or client implementations may guarentee monotonically increasing
    timeseries data. Typically, subscribers are implemented according to the
    client they will proxy subscriptions for. So subscriber implementation may
    depend on the client implementation and source.
    
    Subscribers should never be initialized directly by an API. They should only
    be initialized by an `AbstractManager`.
    
    Args:
        subscriptions: The set of subscriptions this subscriber is responsible
            for.
        manager: The manager instance which initialized the subscriber.
        maxlen: The maximum number of messages that can buffered on the subscriber.
            If the buffer limit on the subscriber is reached, the oldest messages
            will be evicted as new messages are added.

    Examples:
    >>> # The preferred use of a subscriber is with a context manager.
    ... # This will handle calling the `stop` method at the end of the context block
    ... with await manager.subscribe(...) as subscriber:
    ...     async for msg in subscriber:
    ...     ...
    >>> # They can be stopped manually
    ... subscriber = await manager.subscribe(...)
    ... try:
    ...     async for msg in subscriber:
    ...         ...
    ... finally:
    ...     subscriber.stop()
    """
    def __init__(
        self,
        subscriptions: Set[BaseSubscription],
        callback: Callable[["AbstractSubscriber"], None],
        maxlen: int
    ) -> None:
        self.subscriptions = subscriptions
        self.callback = callback
        
        self.loop: asyncio.AbstractEventLoop = None
        self.stop_waiter: asyncio.Future = None
        self.stopped: bool = False
        self.data_queue: Deque[Any] = deque(maxlen=maxlen)

    @property
    def running(self) -> bool:
        """`True` if subscriber can be iterated over."""
        return not self.stopped

    def stop(self) -> None:
        """Stop the subscriber.
        
        This signals the manager to drop the subscriber. Any attempt to iterate
        over the subscriber instance will exhaust the iterator immediately.
        """
        if self.stopped:
            return
        self.stopped = True
        waiter = self.stop_waiter
        self.stop_waiter = None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        try:
            self.loop.call_soon(self.callback, self)
        except RuntimeError:
            # Loop closed
            pass

    @abstractmethod
    def publish(self, data: Any) -> None:
        """Publish data to the subscriber.

        This method may validate the data from the manager before placing it
        into the data queue.
        """

    async def start(self) -> bool:
        """Start the subscriber.
        
        This method ensures the subscriber can be iterated over and adds a
        reference to the event loop. This method *MUST* be called before
        iteration.
        """
        if self.stopped:
            return False
        if self.stop_waiter is not None:
            raise RuntimeError(
                "Cannot iterate over subscriber while another coroutine is "
                "already waiting for the next message"
            )
        loop = asyncio.get_running_loop()
        stop = loop.create_future()
        self.loop = loop
        self.stop_waiter = stop
        return True

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} - {len(self.subscriptions)} subscriptions"

    def __enter__(self) -> "AbstractSubscriber":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        self.stop()

    def __del__(self) -> None:
        self.stop()


class AbstractLock(ABC):
    """Standard interface for a distributed locking mechanism.
    
    Locks are used in certain manager implementations where multiple processes
    could be streaming the same data. Redis and Memcached are two commonly
    used backends for distributed locks.
    
    This class provides a common set of methods so the locks can be used
    interchangeably on different managers.

    Args:
        ttl: The time to live on a subscription lock.
        id_: The value to assign to a lock. The default is random UUID and
            generally this should not be overwritten.
    """
    def __init__(self, ttl: int = 5000, id_: str = uuid.uuid4().hex) -> None:
        self.ttl = ttl
        self.id_ = id_

        self.loop = asyncio.get_event_loop()

    @abstractproperty
    def closed(self) -> bool:
        """`True` is lock is closed and cannot be used."""

    @abstractmethod
    async def close(self) -> None:
        """Close the lock and cleanup any resources. This should only ever be
        called by the manager instance which owns the lock.
        """

    @abstractmethod
    async def acquire(self, subscriptions: Sequence[BaseSubscription]) -> Set[BaseSubscription]:
        """Acquire a lock for a subscription tied to an `AbstractClient` instance.

        Note: `BaseSubscription` has a consistent hash and that key should be
        used as the key in `acquire`.
        
        Args:
            subscriptions: A sequence of subscriptions to try and lock to this
                process.

        Returns:
            subscriptions: The subscriptions for which a lock was successfully
                acquired.
        """
        raise NotImplementedError()

    @abstractmethod
    async def register(self, subscriptions: Sequence[BaseSubscription]) -> None:
        """Register subscriptions tied to an `AbstractSubscriber` instance.
        
        This allows the owner of a subscription lock in a different process to
        poll the locking service and see if a lock which that process owns
        is still required. This method *MUST* extend the TTL on a subscription
        if the key already exists.

        Note: There is a distinct difference between the locks for `acquire`
        and the locks for `register`. For `register` a hashing algorithm *MUST*
        be applied to the hash of the subscription for the key to ensure keys
        are not overwritten

        Args:
            subscriptions: A sequence of subscriptions to register.
        """
        raise NotImplementedError()

    @abstractmethod
    async def release(self, subscriptions: Sequence[BaseSubscription]) -> None:
        """Release a lock for a subscription tied this process.
        
        This method must only be called that a process whos client owns the
        subscription.

        Args:
            subscriptions: A sequence of subscriptions to release an owned lock for.
        """
        raise NotImplementedError()

    @abstractmethod
    async def extend(self, subscriptions: Sequence[BaseSubscription]) -> None:
        """Extend the lock on a subscription owned by this process.
        
        This method must only be called that a process whos client owns the
        subscription.

        Args:
            subscriptions: A sequence of subscriptions to extend an owned lock for.
        """
        raise NotImplementedError()

    @abstractmethod
    async def client_poll(self, subscriptions: Sequence[BaseSubscription]) -> Set[BaseSubscription]:
        """Poll subscriptions tied to the manager's client instance and see if
        those subscriptions are still required.
        
        In a distributed context, a subscriber in one process can be dependent
        on the manager in a different process. So while the owning process for
        a subscription may not require the subscription for any of its
        subscribers, another process may still require it.
        
        This method returns subscriptions which can be unsubscribed from. In other
        words, there is no active subscriber in the cluster requiring that
        subscription.

        Args:
            subscriptions: A sequence of subscriptions which the current process
                is streaming data for.
        
        Returns:
            subscriptions: The subscriptions which are not required anymore.
        """
        raise NotImplementedError()

    @abstractmethod
    def subscriber_poll(self, subscriptions: Sequence[BaseSubscription]) -> Set[BaseSubscription]:
        """Poll subscriptions in this process to ensure data is streaming from
        at least one process in the cluster.
        
        This method returns subscriptions which are not being streamed by a manager
        in the cluster. A manager instance which owns the subscriber may choose
        to subscribe to the missing subscriptions on its client or stop the
        subscriber.

        Args:
            subscriptions: A sequence of subscriptions which the current process
                requires data to be streaming for.

        Returns:
            subscriptions: The subscriptions are not being streamed anywhere
                in the cluster.
        """
        raise NotImplementedError()

    def __del__(self):
        try:
            if not self.closed:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(self.close())
        except Exception:
            pass


class AbstractTimeseriesCollection(AsyncIterable[TimeseriesRow]):
    """Standard interface for a timeseries collection which streams timeseries
    data one or multiple sources.
    
    Rows *MUST* be in chronological order. It is the implementation's responsibility
    to ensure this. Data is streamed relative to the current time
    (i.e "last 15 minutes" -> timedelta(minutes=15)).

    RedisTimeseries is an example backend for the `AbstractTimeseriesCollection`,
    the collection is simply a conduit for the API calls and data processing to
    stream a collection of timeseries in timestamp aligned rows.
    
    Collections are intended to be long lived and reusable, they are always
    streaming the data from the source backend relative to when iteration starts.
    
    Args:
        subscriptions: A sequence of the subscriptions to stream data for. The
            data will be streamed in sorted order of the subscriptions (using
            the hash of the subscription).
        delta: A timedelta for the stream period.
    """
    def __init__(
        self,
        subscriptions: Sequence[BaseSubscription],
        delta: timedelta
    ) -> None:
        self.subscriptions = set(subscriptions)
        self.delta = delta
        self.loop = asyncio.get_event_loop()

    @abstractproperty
    def closed(self) -> bool:
        """`True` if collection is closed and cannot be used."""

    @abstractmethod
    async def close(self) -> None:
        """Close the collection.
        
        Outstanding iterators should end and any resources required for the
        collection should be cleaned up.
        """

    def __del__(self):
        try:
            if not self.closed:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(self.close())
        except Exception:
            pass