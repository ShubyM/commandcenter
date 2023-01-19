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
    """Standard interface for all client instances.
    
    The purpose of a client is to handle all I/O to a particular data source.
    It facilitates this through the `AbstractConnection` interface where the
    actual I/O is occurring.
    
    Managers use client instances to ferry data from the source to a subscriber
    interested in a particular set of subscriptions. Clients must be asynchronous
    and should provide a well defined structure for all incoming messages.
    """

    def __init__(self, max_buffered_messages: int = 1000) -> None:
        self._connections: List["AbstractConnection"] = []
        self._errors_queue: asyncio.Queue = asyncio.Queue()
        self._data_queue: asyncio.Queue = asyncio.Queue(maxsize=max_buffered_messages)
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    
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
    
    @abstractproperty
    def subscriptions(self) -> Set[BaseSubscription]:
        """Return a set of the subscriptions from all connections."""
        subscriptions = set()
        for connection in self._connections: subscriptions.update(connection.subscriptions)
        return subscriptions

    @abstractmethod
    async def close(self) -> None:
        """Close the client instance shutting down all connections."""
        raise NotImplementedError()

    async def messages(self) -> AsyncIterable[str]:
        """Receive incoming messages from all connections.
        
        This is the central point where all messages flow through to be picked
        up by a manager instance. The manager has no concept of the underlying
        workings of the client and its connection structure.
        """
        while True:
            yield await self._data_queue.get()

    async def errors(self) -> AsyncIterable[ErrorMessage]:
        """Receive errors that cause connections to fail.
        
        Errors should include information such as the exception which caused
        the connection to fail and the subscriptions which are affected.
        """
        while True:
            yield await self._errors_queue.get()

    @abstractmethod
    async def subscribe(self, subscriptions: Sequence[BaseSubscription]) -> bool:
        """Subscribe to data points from a source.
        
        This returns a boolean indicating whether or not the operation was
        successful. If `True`, all subscriptions were successfully subscribed,
        if `False` none of the subscriptions were subscribed to.
        
        This method should create the appropriate number of connections to support
        all the subscriptions. It should not interrupt service for any other
        subscriptions already supported by the client.
        """
        raise NotImplementedError()

    @abstractmethod
    async def unsubscribe(self, subscriptions: Sequence[BaseSubscription]) -> bool:
        """Unsubscribe from data points from a source.
        
        This returns a boolean indicating whether or not the operation was
        successful. If `True`, all subscriptions were successfully unsubscribed,
        if `False` none of the subscriptions were unsubscribed from.
        
        This method should stop the appropriate connections and clean up any
        resources as needed. However, it must not interrupt service (i.e stop
        connections) for any other subscriptions already supported by the client.
        """
        raise NotImplementedError()

    @abstractmethod
    def connection_lost(self, connection: "AbstractConnection") -> None:
        """Callback for connection instances to signal when they have stopped.
        
        The `exception` property should be examined on each connection after it
        has stopped. If an unhandled exception caused the connection to stop,
        this should be added to `_errors_queue` in the form of an `ErrorMessage.
        Regardless, the connection must be removed from the `_connections`
        attribute on the client instance.
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


class AbstractConnection(ABC):
    """Standard interface for all connection instances.
    
    A connection is where the actual I/O to a data source occurs. The purpose
    of a connection to abstract those interface specific details from the
    parent client instance.
    
    Connections should never be directly created, they should only be created,
    started, and stopped by the client instance which owns the connection object.
    
    Args:
        client: The parent client instance which owns the connection
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
        runner = self.runner
        return runner is not None and not runner.done()
    
    def connection_lost(self, fut: asyncio.Future) -> None:
        """Callback added to the `_runner` task for when the connection stops.
        
        The connection can be stopped due to the `stop` method in which case, the
        `_runner` task is cancelled. It can also be stopped due to an unhandled
        exception. If an unahdled exception occurred, this method sets the
        `exception` attribute to the exception that caused the runner to stop.
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
        
        If `_online` is `True`, the connection can pass data to the parent client
        instance. If `False` then it cannot.
        """
        if not self.running:
            raise RuntimeError("Attempted to toggle status of non-running connection")
        self.online = not self.online

    @abstractmethod
    async def run(self) -> None:
        """Perform I/O to data source in an infinite loop.
        
        This method is the coroutine for the `_runner` task and should
        receive/retrieve, parse, and validate data from the source which it is
        connecting to.
        
        When the connection status is 'online' (i.e `_online` is `True`) data
        may be passed to the client instance.
        
        This method must raise `asyncio.CancelledError` when cancelled. The exception
        may be caught in order to clean up resources but it must re-raised in
        that case.
        """
        raise NotImplementedError()

    @abstractmethod
    async def start(self, subscriptions: Set[BaseSubscription], feeder: asyncio.Queue) -> None:
        """Start the `_runner` task.
        
        This method may perform any intial connection setup to the data source
        before starting the task.
        
        Any exceptions raised in the `start` method should lead to `False` being
        returned for the `subscribe` and `unsubscribe` methods on the client
        instance.

        Args:
            subscriptions: A set of subscriptions to connect to at the datasource.
            feeder: A queue to place received messages into.
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
        if self.running:
            self.stop()


class AbstractManager(ABC):
    """Standard interface for all manager instances.
    
    The purspose of a manager is to bridge the gap between a client instance,
    which retrieves data from a source, and a subscriber (the consumer of the
    data). The manager can stop subscribers if an error occurs on the client
    and it can also release subscriptions on the client if the subscriber
    disconnects.

    The manager and subscribers should abstract away all the data processing
    and formatting so that a consistent interface and predictable data structure
    is provided to the end user across different data sources. The end user
    should not need to worry about whether the underlying connection type is
    websockets, long polling, etc. To the end user, they are getting an
    asynchronous stream of data from a source.
    
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
        self._subscriber = subscriber
        self._max_subscribers = max_subscribers
        self._maxlen = maxlen

        self._closed: bool = False
        self._subscribers: List[AbstractSubscriber] = []
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    @property
    def closed(self) -> bool:
        """`True` if the manager is closed."""
        return self._closed

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        """Return a set of the subscriptions from all subscribers."""
        subscriptions = set()
        for subscriber in self._subscribers: subscriptions.update(subscriber.subscriptions)
        return subscriptions

    async def close(self) -> None:
        """Stop all subscribers and close the client instance."""
        if not self._closed:
            for subscriber in self._subscribers: subscriber.stop()
            self._subscribers.clear()
            self._closed = True
            await self.client.close()

    @abstractmethod
    async def subscribe(
        self,
        subscriptions: Sequence[BaseSubscription]
    ) -> "AbstractSubscriber":
        """Subscribe to the subscriptions on the client instance and configure
        a subscriber.
        
        If the subscription process on the client fails, this must raise a
        `SubscriptionError`.
        This method should check the capacity of the manager to ensure it can
        support another subscriber. If it cannot, this must raise a `CapacityError`
        """
        raise NotImplementedError()

    @abstractmethod
    def subscriber_lost(self, subscriber: "AbstractSubscriber") -> None:
        """Callback for subscriber instances after their `stop` method was called.
        
        At a minumum, this should remove the subscriber from the list of subscribers
        for the manager. It can additionaly free up resources on the client instance
        or signal other tasks.
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


class AbstractSubscriber(AsyncIterable[Union[str, bytes]]):
    """Standard interface for all subscriber instances.
    
    The purpose of a subscriber is to be single source for asynchronous data
    from a source. Data flows through a subscriber from a manager which bridges
    the gap between subscriber and actual I/O. By extension, subscriber instances
    do not perform any I/O, they are asynchronous iterables.
    
    Subscribers should only ever be created by the manager instance which owns
    them. End user should never directly create subscribers.
    
    The preferred use of a subscriber is with a context manager
    ```python
    with await manager.subscribe(...) as subscriber:
        async for msg in subscriber:
            ...
    ```
    This will handle calling the `stop` method at the end of the context block
    and will signal the manager to drop the subscriber and clean up any resources.
    
    It SHOULD fall on the subscriber implementation to ensure it is only forwarding
    along data for its subscriptions and that all data is in chronological
    order. Some manager/client implementation MAY handle this so the subscriber
    does not need to but ultimately, data produced by a subscriber MUST be only
    for the set of subscriptions the instance is responsible for and the data
    MUST be chronological order.
    
    Args:
        subscriptions: The set of subscriptions this subscriber is responsible
            for.
        manager: The manager instance which initialized the subscriber.
        maxlen: The maximum number of messages that can buffered on the subscriber.
            If the buffer limit on the subscriber is reached, the oldest messages
            will be evicted as new messages are added.
    """
    def __init__(
        self,
        subscriptions: Set[BaseSubscription],
        callback: Callable[["AbstractSubscriber"], None],
        maxlen: int
    ) -> None:
        self.subscriptions = subscriptions
        self._callback = callback
        
        self._loop: asyncio.AbstractEventLoop = None
        self._stop_waiter: asyncio.Future = None
        self._stopped: bool = False
        self._data_queue: Deque[Any] = deque(maxlen=maxlen)

    @property
    def running(self) -> bool:
        waiter = self._stop_waiter
        if waiter is not None:
            return not waiter.done()
        return False

    def stop(self) -> None:
        """Stop the subscriber.
        
        This signals the manager to drop the subscriber and an attempt to iterate
        over the subscriber instance will exhaust the iterator.
        """
        if self._stopped:
            return
        self._stopped = True
        waiter = self._stop_waiter
        self._stop_waiter = None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        try:
            self._loop.call_soon(self._callback, self)
        except RuntimeError:
            # Loop closed
            pass

    def publish(self, data: Any) -> None:
        """Publish data to the subscriber. This method should only be called by
        the manager.
        """
        self._data_queue.append(data)

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
        if self.running:
            self.stop()


class AbstractDistLock(ABC):
    """Standard interface for a distributed locking mechanism to be used alongside
    an `AbstractManager`.
    
    Redis and Memcached are two commonly used systems for distributed locks.
    
    This class provides a common set of methods so the locks can be used
    interchangeably on different managers.
    """
    def __init__(self, ttl: int = 5000, id_: str = uuid.uuid4().hex) -> None:
        self.ttl = ttl
        self.id_ = id_

        self._loop = asyncio.get_event_loop()

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
        
        This returns only the subscriptions which a lock was successfully acquired
        for.
        """
        raise NotImplementedError()

    @abstractmethod
    async def register(self, subscriptions: Sequence[BaseSubscription]) -> Set[BaseSubscription]:
        """Register subscriptions tied to a specific `AbstractSubscriber` instance.
        
        This allows for a different process to poll the locking service and see
        if a lock which that process owns (and consequently, the client subscription)
        is still required.
        
        This method must also extend the TTL on a subscription if this process
        owns the lock.
        """
        raise NotImplementedError()

    @abstractmethod
    async def release(self, subscriptions: Sequence[BaseSubscription]) -> None:
        """Release a lock for a subscription tied to an `AbstractClient` instance.
        
        This method must only be called that a process whos client owns the
        subscription.
        """
        raise NotImplementedError()

    @abstractmethod
    async def extend(self, subscriptions: Sequence[BaseSubscription]) -> None:
        """Extend the lock on a subscription tied to an `AbstractClient` instance.
        
        This method must only be called that a process whos client owns the
        subscription.
        """
        raise NotImplementedError()

    @abstractmethod
    async def client_poll(self, subscriptions: Sequence[BaseSubscription]) -> Set[BaseSubscription]:
        """Poll `AbstractClient` subscriptions to see if a client subscription is
        still required.
        
        In a distributed context, an `AbstractSubscriber` in one process can be
        dependent on an `AbstractClient` in a different process. So while the
        owning process for a subscription on an `AbstractClient` may not require
        the subscription for any of its subscribers, another process may still
        require it.
        
        This method returns subscriptions which can be unsubscribed from. In other
        words, there is no active `AbstractSubscriber` requiring that subscription.
        """
        raise NotImplementedError()

    @abstractmethod
    def subscriber_poll(self, subscriptions: Sequence[BaseSubscription]) -> Set[BaseSubscription]:
        """Poll `AbstractSubscriber` subscriptions to ensure a client in the cluster
        is streaming data for the subscriptions.
        
        This method returns subscriptions which are not being streamed by a client
        in the cluster. A manager instance which owns the subscriber may choose
        to subscribe to the missing subscriptions on its client or stop the
        subscriber.
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
    data from a source.
    
    RedisTimeseries is great backend for the `AbstractTimeseriesCollection`, the
    collection is simply a conduit for the API calls and data processing to
    stream a collection of timeseries in timestamp aligned rows.
    
    Rows MUST be in chronological order. Data is streamed relative to the
    current time (i.e "last 15 minutes" (timedelta(minutes=15))).
    
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
        self._delta = delta
        self._loop = asyncio.get_event_loop()

    @abstractproperty
    def closed(self) -> bool:
        """`True` is collection is closed and cannot be used."""

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