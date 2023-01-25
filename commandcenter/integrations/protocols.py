import asyncio
from collections.abc import AsyncIterable
from types import TracebackType
from typing import (
    Any,
    AsyncIterable,
    Protocol,
    Sequence,
    Set,
    Type,
)

from commandcenter.integrations.models import BaseSubscription, DroppedConnection



class Client(Protocol):
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
        """Close the client instance and shut down all connections."""
        ...

    async def dropped(self) -> AsyncIterable[DroppedConnection]:
        """Receive messages for dropped connections."""
        yield

    async def messages(self) -> AsyncIterable[str]:
        """Receive incoming messages from all connections."""
        yield

    def subscribe(self, subscriptions: Sequence[BaseSubscription]) -> None:
        """Subscribe to a sequence of subscriptions.

        Args:
            subscriptions: The subscriptions to subscribe to.

        Returns:
            status: A boolean indicating whether or not the operation was
                successful. If `True`, *all* subscriptions were successfully
                subscribed, if `False` *none* of the subscriptions were subscribed
                to.
        """
        ...

    def unsubscribe(self, subscriptions: Sequence[BaseSubscription]) -> None:
        """Unsubscribe from from a sequence of subscriptions.
        
        Args:
            subscriptions: The subscriptions to unsubscribe from.
        """
        ...

    def connection_lost(self, fut: asyncio.Future) -> None:
        """Callback after connections have stopped."""
        ...


class Connection(Protocol):
    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        """Return a set of the subscriptions for this connections."""
        ...

    async def run(self, *args: Any, **kwargs: Any) -> None:
        """Main implementation for the connection.
        
        This method should receive/retrieve, parse, and validate data from the
        source which it is connecting to.
        
        When the connection status is 'online' data may be passed to the client.
        """
        ...

    async def start(
        self,
        subscriptions: Set[BaseSubscription],
        data_queue: asyncio.Queue,
        *args: Any,
        **kwargs: Any
    ) -> None:
        """Start the connection. This is called as a task by the client.

        Args:
            subscriptions: A set of subscriptions to connect to at the datasource.
            data_queue: A queue where processed data is be placed.
        """
        ...


class Manager(Protocol):
    @property
    def ready(self) -> asyncio.Event:
        """Awaitable that controls flow based on core background tasks in the
        manager.
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
    ) -> "Subscriber":
        """Subscribe on the client and configure a subscriber.
        
        Args:
            subscriptions: The subscriptions to subscribe to.
        """
        ...
    
    def subscriber_lost(self, fut: asyncio.Future) -> None:
        """Callback after subscribers have stopped."""
        ...


class Subscriber(Protocol):
    @property
    def stopped(self) -> bool:
        """`True` if subscriber cannot be iterated over."""
        ...

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        """Return a set of the subscriptions for this subscriber."""
        ...

    def stop(self, e: Exception | None) -> None:
        """Stop the subscriber."""
        ...

    def publish(self, data: str) -> None:
        """Publish data to the subscriber."""
        ...

    async def start(self, subscriptions: Set[BaseSubscription]) -> None:
        """Start the subscriber.
        
        This method is called as a task by the manager.
        """
        ...

    async def __aiter__(self) -> AsyncIterable[str]:
        ...

    def __enter__(self) -> "Subscriber":
        ...

    def __exit__(
        self,
        exc_type: Type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        ...


class Lock:
    @property
    def closed(self) -> bool:
        """`True` is lock is closed and cannot be used."""
        ...

    async def close(self) -> None:
        """Close the lock."""
        ...

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
        ...

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