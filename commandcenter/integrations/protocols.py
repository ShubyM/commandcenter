import asyncio
from collections.abc import AsyncIterable
from types import TracebackType
from typing import (
    Any,
    AsyncIterable,
    Optional,
    Protocol,
    Sequence,
    Set,
    Type,
)

from commandcenter.integrations.models import BaseSubscription, ErrorMessage
from commandcenter.integrations.types import TimeseriesRow



class IntegrationClient(Protocol):
    """Standard protocol for all real-time integration client instances.
    
    A client manages a pool of connections to a data source. It behaves as a
    subscription interface.
    
    Managers use client instances to ferry data from the source to a subscriber
    interested in a particular set of subscriptions. Clients should rarely
    (if ever) be handled outside the scope of a manager.
    """
    
    @property
    def capacity(self) -> int:
        """Return an integer indicating how many more subscriptions this client
        can support.
        """
        ...

    @property
    def closed(self) -> bool:
        """Returns `True` if client is closed. A closed client has no connections
        and cannot accept subscriptions.
        """
        ...
    
    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        """Return a set of the subscriptions from all connections."""
        ...
    
    async def close(self) -> None:
        """Close the client instance and shut down all connections.
        
        Any resources having to do with underlying I/O transport must be finalized
        here.
        """
        ...

    async def errors(self) -> AsyncIterable[ErrorMessage]:
        """Receive errors that caused connections to fail.
        
        A failed connection equates to a loss of service which the manager
        must be notified about so that it can shut down any subscribers
        dependent on those subscriptions.
        """
        ...

    async def messages(self) -> AsyncIterable[str]:
        """Receive incoming messages from all connections.
        
        This is the central point for data flow through the client to the manager.
        The manager has no concept of the underlying workings of the client and
        its connection structure.
        """
        ...

    async def subscribe(self, subscriptions: Sequence[BaseSubscription]) -> bool:
        """Subscribe to subscriptions for a source.
        
        This method should create the appropriate number of connections to support
        all the subscriptions. Also, it *should* not interrupt service for any other
        subscriptions.

        Args:
            subscriptions: The subscriptions to subscribe to.

        Returns:
            status: A boolean indicating whether or not the operation was
                successful. If `True`, all subscriptions were successfully
                subscribed, if `False` *none* of the subscriptions were subscribed
                to.
        """
        ...

    async def unsubscribe(self, subscriptions: Sequence[BaseSubscription]) -> bool:
        """Unsubscribe from subscriptions for a source.
        
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
        ...

    def connection_lost(self, connection: "IntegrationConnection") -> None:
        """Callback for `IntegrationConnections` after they have stopped.
        
        If the connection stopped due to an unhandled exception, the exception
        *must* be packaged into an `ErrorMessage` and placed in the errors queue
        to be picked up by the manager.
        """
        ...


class IntegrationConnection(Protocol):
    """Standard protocol for all real-time integration connection instances.
    
    A connection is where the actual I/O to a source occurs. A connection should
    abstract away the underlying protocol client.
    
    Connections are always slaves to a client and should never be created outside
    the scope of a client.
    """
    @property
    def running(self) -> bool:
        """`True` if the connection is active."""
        ...
    
    def connection_lost(self, fut: asyncio.Future) -> None:
        """Callback method that *must* be called when the connection stops. This
        should set any states or exception propertys then call the client's
        connection lost method.
        """
        ...

    def stop(self) -> None:
        """Stop the connection. This method is idempotent, multiple calls to
        `stop` will have no effect.
        """
        ...
    
    def toggle(self) -> None:
        """Toggle the status of the connection.
        
        Only if a connection is an 'on' or 'online' state can it pass data to the
        client otherwise, the messages *must* be discarded.
        """
        ...

    async def run(self) -> None:
        """Main task of `IntegrationConnection`.
        
        This task is started in the background by the `start` method. It should
        receive/retrieve, parse, and validate data from the source which it is
        connecting to.
        
        When the connection status is 'online' data may be passed to the client.
        
        This method must raise `asyncio.CancelledError` when cancelled. The exception
        may be caught in order to clean up resources but it must re-raised.
        """
        ...

    async def start(self, subscriptions: Set[BaseSubscription]) -> None:
        """Start the `run` task in the backgorund.

        This method may perform any intial connection setup to the data source
        before starting the task.
        
        Any exceptions raised in the `start` method should not be suppressed,
        they will be handled in the client.

        Args:
            subscriptions: A set of subscriptions to connect to at the datasource.
        """
        ...



class IntegrationManager(Protocol):
    """Standard protocol for all manager integrations.
    
    A manager bridges the gap between a client integration, which retrieves data
    from a source, and a subscriber, the consumer of the data.
    
    Managers are designed to support any client instance which implements the
    `IntegrationClient` interfaces.
    
    Managers are similar to fanout exchange where a client is a publisher and
    manager distributes published messages to all subscribers.

    The manager-subscriber model abstracts away all the protocol crap from
    data integrations. It is designed to be a backplane for consistent data
    structures and allow subscribers streaming from multiple sources to merge
    those streams in a trivial way.
    """

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        """Return a set of the subscriptions from all subscribers."""
        ...

    async def close(self) -> None:
        """Stop all subscribers and close the client."""
        ...

    async def subscribe(
        self,
        subscriptions: Sequence[BaseSubscription]
    ) -> "IntegrationSubscriber":
        """Subscribe to the subscriptions on the client and configure a subscriber.
        
        If the subscription process on the client fails, this *must* raise a
        `SubscriptionError`.

        This method *must* check the capacity of the manager to ensure it can
        support another subscriber. If it cannot, it *must* raise a `CapacityError`
        """
        ...

    def subscriber_lost(self, subscriber: "IntegrationSubscriber") -> None:
        """Callback for a subscriber after their `stop` method was called.
        
        At a minumum, this *must* remove the subscriber from the active subscribers
        for the manager. It *should* signal other tasks to check the client and see
        if its subscriptions are still required.
        """
        ...


class IntegrationSubscriber(Protocol):
    """Standard protocol for all subscriber integrations.
    
    Subscribers stream data published to them from a manager. Subscribers are
    the final gatekeeper for data integrity and validation. They have two
    requirements.
    
    1. Subscribers *must* ensure timeseries data is in monotonically increasing
    order.
    2. Subscribers *must* ensure they only proxy subscriptions which they are
    responsible for.
    3. Subscriber *must* ensure they dont send duplicate data. 
    
    Depending on the manager implementation, there is no guarentee that
    a message received in the subscribers queue is intended for this subscriber.
    There is also no guarentee that a manager will not occassionally send a duplicate
    message especially in a distributed environment.

    Some manager or client implementations may guarentee monotonically increasing
    timeseries data. Typically, subscribers are implemented according to the
    client they will proxy subscriptions for. So subscriber implementation may
    depend on the client implementation and source.
    
    Subscribers are always slaves to a manager and should never be created outside
    the scope of a manager.

    Examples:
    The preferred use of a subscriber is with a context manager. This must handle
    calling the `stop` method at the end of the context block...
    >>> with await manager.subscribe(...) as subscriber:
    ...     async for msg in subscriber:
    ...     ...

    They can also be stopped manually...
    >>> subscriber = await manager.subscribe(...)
    >>> try:
    ...     async for msg in subscriber:
    ...         ...
    ... finally:
    ...     subscriber.stop()
    """

    @property
    def stopped(self) -> bool:
        """`True` if subscriber cannot be iterated over."""
        ...

    def stop(self) -> None:
        """Stop the subscriber.
        
        This must signal the manager to drop the subscriber. Any attempt to iterate
        over the subscriber instance will exhaust the iterator immediately after
        the stop method has been called.
        """
        ...

    def publish(self, data: Any) -> None:
        """Publish data to the subscriber.

        This method may validate the data from the manager before placing it
        into the data queue.

        This method should only be called by the manager.
        """
        ...

    async def start(self) -> bool:
        """Start the subscriber.
        
        This method ensures the subscriber can be iterated over and sets up the
        mechanism for stopping the subscriber during iteration.
        """

    async def __aiter__(self) -> AsyncIterable[str | bytes]:
        ...

    def __enter__(self) -> "IntegrationSubscriber":
        ...

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        ...


class IntegrationLock:
    """Standard protocol for a distributed locking mechanism.
    
    Locks are used in certain manager implementations where multiple processes
    could be streaming the same data. Redis and Memcached are two commonly
    used backends for distributed locks.
    
    This class provides a common set of methods so the locks can be used
    interchangeably on different managers.
    """

    @property
    def closed(self) -> bool:
        """`True` is lock is closed and cannot be used."""

    async def close(self) -> None:
        """Release any locked resources and close the lock.
        
        This should only ever be called by the manager instance which owns the lock.
        """

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
        ...

    async def register(self, subscriptions: Sequence[BaseSubscription]) -> None:
        """Register subscriptions tied to an `AbstractSubscriber` instance.
        
        This allows the owner of a subscription lock in a different process to
        poll the locking service and see if a lock which that process owns
        is still required. This method *must* extend the TTL on a subscription
        if the key already exists.

        Note: There is a distinct difference between the locks for `acquire`
        and the locks for `register`. For `register` a hashing algorithm *must*
        be applied to the hash of the subscription for the key to ensure keys
        are not overwritten.

        Args:
            subscriptions: A sequence of subscriptions to register.
        """

    async def release(self, subscriptions: Sequence[BaseSubscription]) -> None:
        """Release a lock for a subscription tied this process.
        
        This method must only be called that a process whos client owns the
        subscription.

        Args:
            subscriptions: A sequence of subscriptions to release an owned lock for.
        """
        ...

    async def extend(self, subscriptions: Sequence[BaseSubscription]) -> None:
        """Extend the lock on a client subscription owned by this process.
        
        This method must only be called that a process whos client owns the
        subscription.

        Args:
            subscriptions: A sequence of subscriptions to extend an owned lock for.
        """
        ...

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
        ...

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
        ...


class AbstractTimeseriesCollection(Protocol):
    """Standard protocol for a timeseries collection which streams timeseries
    data from one or multiple sources.
    
    Rows *must* be in monotonically increasing order. It is the implementation's
    responsibility to ensure this. Data is streamed relative to the current time
    (i.e "last 15 minutes" -> timedelta(minutes=15)).

    RedisTimeseries is an example backend for the `AbstractTimeseriesCollection`,
    the collection is simply a conduit for the API calls and data processing to
    stream a collection of timeseries in timestamp aligned rows.
    
    Collections are intended to be long lived and reusable, they are always
    streaming the data from the source backend relative to when iteration starts.
    """

    @property
    def closed(self) -> bool:
        """`True` if collection is closed and cannot be used."""

    async def close(self) -> None:
        """Close the collection.
        
        Outstanding iterators should end and any resources required for the
        collection should be cleaned up.
        """

    async def __aiter__(self) -> AsyncIterable[TimeseriesRow]:
        ...