import asyncio
from collections.abc import AsyncIterable, Sequence
from types import TracebackType
from typing import Any, Deque, Protocol, Set, Type

from commandcenter.integrations.models import (
    BaseSubscription,
    DroppedSubscriptions,
    SubscriberCodes
)



class Client(Protocol):
    @property
    def capacity(self) -> int:
        """Returns an integer indicating how many more subscriptions this client
        can support.
        """
        ...

    @property
    def closed(self) -> bool:
        """Returns `True` if client is closed. A closed client cannot accept
        new subscriptions.
        """
        ...
    
    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        """Returns a set of the subscriptions from all connections."""
        ...
    
    async def close(self) -> None:
        """Close the client instance and shut down all connections."""
        ...

    async def dropped(self) -> AsyncIterable[DroppedSubscriptions]:
        """Receive messages for dropped connections."""
        yield

    async def messages(self) -> AsyncIterable[str]:
        """Receive incoming messages from all connections."""
        yield

    async def subscribe(self, subscriptions: Set[BaseSubscription]) -> bool:
        """Subscribe to a set of subscriptions.

        Args:
            subscriptions: The subscriptions to subscribe to.

        Returns:
            bool: If `True`, all subscriptions were subscribed to. If `False`,
                no subscriptions were subscribed to.
        """
        ...

    async def unsubscribe(self, subscriptions: Set[BaseSubscription]) -> bool:
        """Unsubscribe from from a set of subscriptions.
        
        Args:
            subscriptions: The subscriptions to unsubscribe from.

        Returns:
            bool: If `True`, all subscriptions were unsubscribed from. If `False`,
                no subscriptions were unsubscribed from.
        """
        ...

    def connection_lost(self, fut: asyncio.Future) -> None:
        """Callback after connections have stopped."""
        ...


class Connection(Protocol):
    @property
    def online(self) -> bool:
        """Returns `True` if the connection is 'online' and is allowed to pubish
        data to the client.
        """
        ...

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        """Return a set of the subscriptions for this connections."""
        ...

    def toggle(self) -> None:
        """Toggle the online status of the connection."""
        ...

    async def run(self, confirm: asyncio.Future, *args: Any, **kwargs: Any) -> None:
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
    ) -> asyncio.Future:
        """Start the connection.

        Args:
            subscriptions: A set of subscriptions to connect to at the data
                source.
            data_queue: A queue where processed data is put.
        
        Returns:
            fut: The running task in the background. If cancelled, this must
                close the connection.
        """
        ...


class Manager(Protocol):
    @property
    def closed(self) -> bool:
        """Returns `True` if the manager is closed. A closed manager cannot
        accept new subscriptions.
        """
        ...

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
        
        Returns:
            subscriber: The configured subscriber.
        """
        ...
    
    def subscriber_lost(self, fut: asyncio.Future) -> None:
        """Callback after subscribers have stopped."""
        ...


class Subscriber(Protocol):
    @property
    def data(self) -> Deque[str]:
        """Returns the data buffer for this subscriber."""
        ...

    @property
    def stopped(self) -> bool:
        """Returns `True` if subscriber cannot be iterated over."""
        ...

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        """Returns a set of the subscriptions for this subscriber."""
        ...

    def publish(self, data: str | bytes) -> None:
        """Publish data to the subscriber.
        
        This method is called by the manager.
        """
        ...
    
    def start(self, subscriptions: Set[BaseSubscription], maxlen: int) -> asyncio.Future:
        """Start the subscriber.
        
        This method is called by the manager.
        """
        ...

    def stop(self, e: Exception | None) -> None:
        """Stop the subscriber."""
        ...

    async def wait(self) -> SubscriberCodes:
        """Wait for new data to be published."""
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
    def ttl(self) -> float:
        """The TTL used for locks in seconds."""
        ...

    async def acquire(self, subscriptions: Set[BaseSubscription]) -> Set[BaseSubscription]:
        """Acquire a lock for a subscription tied to a client.
        
        Args:
            subscriptions: A sequence of subscriptions to try and lock to this
                process.

        Returns:
            subscriptions: The subscriptions for which a lock was successfully
                acquired.
        """
        ...

    async def register(self, subscriptions: Set[BaseSubscription]) -> None:
        """Register subscriptions tied to a subscriber.

        Args:
            subscriptions: A sequence of subscriptions to register.
        """
        ...

    async def release(self, subscriptions: Set[BaseSubscription]) -> None:
        """Release the locks for subscriptions owned by this process.

        Args:
            subscriptions: A sequence of subscriptions to release an owned lock for.
        """
        ...

    async def extend_client(self, subscriptions: Set[BaseSubscription]) -> None:
        """Extend the locks on client subscriptions owned by this process.

        Args:
            subscriptions: A sequence of subscriptions to extend an owned lock for.
        """
        ...

    async def extend_subscriber(self, subscriptions: Set[BaseSubscription]) -> None:
        """Extend the registration on subscriber subscriptions owned by this process.

        Args:
            subscriptions: A sequence of subscriptions to extend a registration for.
        """
        ...

    async def client_poll(self, subscriptions: Set[BaseSubscription]) -> Set[BaseSubscription]:
        """Poll subscriptions tied to the manager's client.
        
        This method returns subscriptions which can be unsubscribed from.

        Args:
            subscriptions: A sequence of subscriptions which the current process
                is streaming data for.
        
        Returns:
            subscriptions: The subscriptions that can unsubscribed from.
        """
        ...

    async def subscriber_poll(self, subscriptions: Set[BaseSubscription]) -> Set[BaseSubscription]:
        """Poll subscriptions tied to the managers subscribers.
        
        This method returns subscriptions which are not being streamed by a manager
        in the cluster. A manager which owns the subscriber may choose to
        subscribe to the missing subscriptions (after it acquires a lock), or
        stop the subscriber.

        Args:
            subscriptions: A sequence of subscriptions which the current process
                requires data to be streaming for.

        Returns:
            subscriptions: The subscriptions that are not being streamed anywhere
                in the cluster.
        """
        ...

    def subscriber_key(self, subscription: BaseSubscription) -> str:
        """Return the subscriber key from the subscription hash."""
        ...