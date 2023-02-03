import asyncio
import hashlib
import logging
import random
from collections import deque
from collections.abc import AsyncIterable, Sequence
from contextlib import suppress
from datetime import datetime
from types import TracebackType
from typing import Any, Deque, Dict, Set, Type

import anyio

from commandcenter.integrations.exceptions import (
    ClientClosed,
    ClientSubscriptionError,
    ManagerClosed,
    SubscriptionError,
    SubscriptionLimitError,
    SubscriptionLockError,
    SubscriptionTimeout
)
from commandcenter.integrations.models import (
    ClientInfo,
    ConnectionInfo,
    DroppedSubscriptions,
    ManagerInfo,
    SubscriberCodes,
    SubscriberInfo,
    Subscription
)
from commandcenter.integrations.protocols import (
    Client,
    Connection,
    Lock,
    Manager,
    Subscriber
)
from commandcenter.types import JSONContent



_LOGGER = logging.getLogger("commandcenter.integrations")


class BaseClient(Client):
    """Base implementation for a client.
    
    Args:
        max_buffered_messages: The max length of the data queue for the client.
    """
    def __init__(self, max_buffered_messages: int = 1000) -> None:
        self._connections: Dict[asyncio.Task, Connection] = {}
        self._data: asyncio.Queue = asyncio.Queue(maxsize=max_buffered_messages)
        self._dropped: asyncio.Queue = asyncio.Queue()
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

        self._created = datetime.now()
        self._connections_serviced = 0

    @property
    def capacity(self) -> int:
        raise NotImplementedError()

    @property
    def closed(self) -> bool:
        raise NotImplementedError()

    @property
    def info(self) -> Dict[str, JSONContent]:
        connection_info = [
            {"id": fut.get_name(), "info": connection.info}
            for fut, connection in self._connections.items()
        ]
        return ClientInfo(
            name=self.__class__.__name__,
            closed=self.closed,
            data_queue_size=self._data.qsize(),
            dropped_connection_queue_size=self._dropped.qsize(),
            created=self._created,
            uptime=(datetime.now() - self._created).total_seconds(),
            active_connections=len(self._connections),
            active_subscriptions=len(self.subscriptions),
            subscription_capacity=self.capacity,
            total_connections_serviced=self._connections_serviced,
            connection_info=connection_info
        ).dict()

    @property
    def subscriptions(self) -> Set[Subscription]:
        subscriptions = set()
        for fut, connection in self._connections.items():
            if not fut.done():
                subscriptions.update(connection.subscriptions)
        return subscriptions

    def clear(self) -> None:
        try:
            while True:
                self._data.get_nowait()
                self._data.task_done()
        except asyncio.QueueEmpty:
            return

    async def close(self) -> None:
        raise NotImplementedError()

    async def dropped(self) -> AsyncIterable[DroppedSubscriptions]:
        while not self.closed:
            msg = await self._dropped.get()
            self._dropped.task_done()
            yield msg
        else:
            raise ClientClosed()
    
    async def messages(self) -> AsyncIterable[str]:
        while not self.closed:
            msg = await self._data.get()
            self._data.task_done()
            yield msg
        else:
            raise ClientClosed()

    async def subscribe(self, subscriptions: Set[Subscription]) -> bool:
        raise NotImplementedError()

    async def unsubscribe(self, subscriptions: Set[Subscription]) -> bool:
        raise NotImplementedError()

    def connection_lost(self, fut: asyncio.Future) -> None:
        assert fut in self._connections
        connection = self._connections.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        # If a connection was cancelled by the client and the subscriptions were
        # replaced through another connection, subscriptions will be empty set
        msg = DroppedSubscriptions(
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
        self._subscriptions: Set[Subscription] = set()
        self._data: asyncio.Queue = None
        self._online: bool = False
        self._started: asyncio.Event = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

        self._created = datetime.now()
        self._total_published = 0

    @property
    def info(self) -> Dict[str, JSONContent]:
        return ConnectionInfo(
            name=self.__class__.__name__,
            online=self.online,
            created=self._created,
            uptime=(datetime.now() - self._created).total_seconds(),
            total_published_messages=self._total_published,
            total_subscriptions=len(self._subscriptions)
        ).dict()

    @property
    def online(self) -> bool:
        return self._online

    @property
    def subscriptions(self) -> Set[Subscription]:
        return self._subscriptions

    def toggle(self) -> None:
        self._online = not self._online

    async def run(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError()

    async def start(
        self,
        subscriptions: Set[Subscription],
        data: asyncio.Queue,
        *args: Any,
        **kwargs: Any
    ) -> asyncio.Future:
        raise NotImplementedError()


class BaseManager(Manager):
    """Base implementation for a manager.

    Args:
        client: The client instance which connects to and streams data from the
            data source.
        subscriber: The subscriber type to use for this manager.
        max_subscribers: The maximum number of concurrent subscribers which can
            run by a single manager. If the limit is reached, the manager will
            refuse the attempt and raise a `SubscriptionLimitError`.
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

        self._background: Set[asyncio.Task] = set()
        self._subscribers: Dict[asyncio.Task, Subscriber] = {}
        self._event: asyncio.Event = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

        self._created = datetime.now()
        self._subscribers_serviced = 0

    @property
    def closed(self) -> None:
        raise NotImplementedError()

    @property
    def info(self) -> Dict[str, JSONContent]:
        client_info = self._client.info
        subscriber_info = [
            {"id": str(id(fut)), "info": subscriber.info}
            for fut, subscriber in self._subscribers.items()
        ]
        return ManagerInfo(
            name=self.__class__.__name__,
            closed=self.closed,
            created=self._created,
            uptime=(datetime.now() - self._created).total_seconds(),
            active_subscribers=len(self._subscribers),
            active_subscriptions=len(self.subscriptions),
            subscriber_capacity=self._max_subscribers - len(self._subscribers),
            total_subscribers_serviced=self._subscribers_serviced,
            client_info=client_info,
            subscriber_info=subscriber_info
        ).dict()

    @property
    def subscriptions(self) -> Set[Subscription]:
        subscriptions = set()
        for fut, subscriber in self._subscribers.items():
            if not fut.done():
                subscriptions.update(subscriber.subscriptions)
        return subscriptions

    async def close(self) -> None:
        for fut in self._subscribers.keys(): fut.cancel()
        if not self._client.closed:
            await self._client.close()

    async def subscribe(self, subscriptions: Sequence[Subscription]) -> "Subscriber":
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


class BaseDistributedManager(BaseManager):
    """Base implementation for a manager in a distributed environment.
    
    Args:
        client: The client instance which connects to and streams data from the
            data source.
        subscriber: The subscriber type to use for this manager.
        lock: The subscription lock.
        max_subscribers: The maximum number of concurrent subscribers which can
            run by a single manager. If the limit is reached, the manager will
            refuse the attempt and raise a `SubscriptionLimitError`.
        maxlen: The maximum number of messages that can buffered on the subscriber.
            If the buffer limit on the subscriber is reached, the oldest messages
            will be evicted as new messages are added.
        timeout: The time to wait for the backing service to be ready before
            rejecting the subscription request.
    """
    def __init__(
        self,
        client: Client,
        subscriber: Type[Subscriber],
        lock: "BaseLock",
        max_subscribers: int = 100,
        maxlen: int = 100,
        timeout: float = 5
    ) -> None:
        super().__init__(client, subscriber, max_subscribers, maxlen)
        self._lock = lock
        self._timeout = timeout

        self._ready: asyncio.Event = asyncio.Event()

    async def subscribe(
        self,
        subscriptions: Sequence[Subscription]
    ) -> Subscriber:
        """Subscribe on the client and configure a subscriber.
        
        Args:
            subscriptions: The subscriptions to subscribe to.
        
        Returns:
            subscriber: The configured subscriber.
        
        Raises:
            ManagerClosed: Cannot subscribe, manager is closed.
            SubscriptionError: Cannot subscribe.
        """
        if self.closed:
            raise ManagerClosed()
        if len(self._subscribers) >= self._max_subscribers:
            raise SubscriptionLimitError(self._max_subscribers)
        
        subscriptions = set(subscriptions)

        try:
            await asyncio.wait_for(self._ready.wait(), timeout=self._timeout)
        except asyncio.TimeoutError as e:
            raise SubscriptionTimeout("Timed out waiting for service to be ready.") from e
        
        await self._lock.register(subscriptions)
        await self._subscribe(subscriptions)

        subscriber = self._subscriber()
        fut = subscriber.start(subscriptions, self._maxlen)
        fut.add_done_callback(self.subscriber_lost)
        self._subscribers[fut] = subscriber

        _LOGGER.debug("Added subscriber %i of %i", len(self._subscribers), self._max_subscribers)
        return subscriber

    async def _subscribe(self, subscriptions: Set[Subscription]) -> None:
        try:
            to_subscribe = await self._lock.acquire(subscriptions)
        except Exception as e:
            raise SubscriptionLockError("Unable to acquire locks") from e
        else:
            _LOGGER.debug("Acquired %i locks", len(to_subscribe))

        if to_subscribe:
            try:
                subscribed = await self._client.subscribe(to_subscribe)
            except ClientClosed as e:
                await self._lock.release(to_subscribe)
                await self.close()
                raise ManagerClosed() from e

            if not subscribed:
                await self._lock.release(to_subscribe)
                raise ClientSubscriptionError("Client refused subscriptions.")

    async def _run(self, *args: Any, **kwargs: Any) -> None:
        """Start and manage background services for manager."""
        raise NotImplementedError()

    async def _manage_subscriptions(self) -> None:
        """Background task that manages subscription locking along with client
        and subscriber subscriptions.
        """
        async with anyio.create_task_group() as tg:
            tg.start_soon(self._get_dropped_subscriptions)
            tg.start_soon(self._extend_client_subscriptions)
            tg.start_soon(self._extend_subscriber_subscriptions)
            tg.start_soon(self._poll_client_subscriptions)
            tg.start_soon(self._poll_subscriber_subscriptions)

    async def _get_dropped_subscriptions(self) -> None:
        """Retrieve dropped subscriptions and release client locks."""
        async for msg in self._client.dropped():
            subscriptions = msg.subscriptions
            if subscriptions:
                if msg.error:
                    _LOGGER.warning(
                        "Releasing %i locks due to client connection error",
                        len(subscriptions),
                        exc_info=msg.error
                    )
                else:
                    _LOGGER.debug("Releasing %i locks", len(subscriptions))

                await self._lock.release(subscriptions)

    async def _extend_client_subscriptions(self) -> None:
        """Extend client locks owned by this process."""
        while True:
            sleep = (self._lock.ttl*1000//2 - random.randint(0, self._lock.ttl*1000//4))/1000
            await asyncio.sleep(sleep)
            subscriptions = self._client.subscriptions
            if subscriptions:
                await self._lock.extend_client(subscriptions)

    async def _extend_subscriber_subscriptions(self) -> None:
        """Extend subscriber registrations owned by this process."""
        while True:
            sleep = (self._lock.ttl*1000//2 - random.randint(0, self._lock.ttl*1000//4))/1000
            await asyncio.sleep(sleep)
            subscriptions = self.subscriptions
            if subscriptions:
                await self._lock.extend_subscriber(subscriptions)

    async def _poll_client_subscriptions(self) -> None:
        """Poll client subscriptions owned by this process."""
        while True:
            sleep = (self._lock.ttl + random.randint(0, self._lock.ttl*1000//2))/1000
            await asyncio.sleep(sleep)
            subscriptions = self._client.subscriptions
            if subscriptions:
                unsubscribe = await self._lock.client_poll(subscriptions)
                if unsubscribe:
                    _LOGGER.info("Unsubscribing from %i subscriptions", len(unsubscribe))
                    fut = self._loop.create_task(self._client.unsubscribe(unsubscribe))
                    fut.add_done_callback(self._background.discard)
                    self._background.add(fut)
    
    async def _poll_subscriber_subscriptions(self) -> None:
        """Poll subscriber subscriptions owned by this process."""
        while True:
            sleep = (self._lock.ttl + random.randint(0, self._lock.ttl*1000//2))/1000
            await asyncio.sleep(sleep)
            subscriptions = self.subscriptions
            if subscriptions:
                not_subscribed = await self._lock.subscriber_poll(subscriptions)
                if not_subscribed:
                    try:
                        await self._subscribe(not_subscribed)
                    except SubscriptionError:
                        for fut, subscriber in self._subscribers.items():
                            if (
                                not fut.done() and
                                not_subscribed.difference(subscriber.subscriptions) != not_subscribed
                            ):
                                fut.cancel()
                                _LOGGER.warning(
                                    "Subscriber dropped. Unable to pick up lost subscriptions",
                                    exc_info=True
                                )


class BaseSubscriber(Subscriber):
    """Base implementation for a subscriber."""
    def __init__(self) -> None:
        self._subscriptions = set()
        self._data: Deque[str | bytes] = None
        self._data_waiter: asyncio.Future = None
        self._stop_waiter: asyncio.Future = None
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

        self._created = datetime.now()
        self._total_published = 0

    @property
    def data(self) -> Deque[str | bytes]:
        return self._data

    @property
    def info(self) -> Dict[str, JSONContent]:
        return SubscriberInfo(
            name=self.__class__.__name__,
            stopped=self.stopped,
            created=self._created,
            uptime=(datetime.now() - self._created).total_seconds(),
            total_published_messages=self._total_published,
            total_subscriptions=len(self.subscriptions)
        ).dict()

    @property
    def stopped(self) -> bool:
        return self._stop_waiter is None or self._stop_waiter.done()

    @property
    def subscriptions(self) -> Set[Subscription]:
        return self._subscriptions

    def stop(self, e: Exception | None) -> None:
        waiter, self._stop_waiter = self._stop_waiter, None
        if waiter is not None and not waiter.done():
            _LOGGER.debug("%s stopped", self.__class__.__name__)
            if e is not None:
                waiter.set_exception(e)
            else:
                waiter.set_result(None)

    def publish(self, data: str | bytes) -> None:
        assert self._data is not None
        self._data.append(data)
        
        waiter, self._data_waiter = self._data_waiter, None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        
        self._total_published += 1
        _LOGGER.debug("Message published to %s", self.__class__.__name__)
    
    def start(self, subscriptions: Set[Subscription], maxlen: int) -> asyncio.Future:
        assert self._stop_waiter is None
        assert self._data is None
        
        self._subscriptions.update(subscriptions)
        self._data = deque(maxlen=maxlen)
        
        waiter = self._loop.create_future()
        self._stop_waiter = waiter
        return waiter

    async def wait(self) -> None:
        if self._data_waiter is not None:
            raise RuntimeError("Two coroutines cannot wait for data simultaneously.")
        
        if self.stopped:
            return SubscriberCodes.STOPPED
        
        stop = self._stop_waiter
        waiter = self._loop.create_future()
        self._data_waiter = waiter
        try:
            await asyncio.wait([waiter, stop], return_when=asyncio.FIRST_COMPLETED)
            
            if not waiter.done(): # Stop called
                _LOGGER.debug("%s stopped waiting for data", self.__class__.__name__)
                return SubscriberCodes.STOPPED
            return SubscriberCodes.DATA
        finally:
            waiter.cancel()
            self._data_waiter = None

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


class BaseLock(Lock):
    """Base implementation for a lock."""

    def subscriber_key(self, subscription: Subscription) -> str:
        o = str(hash(subscription)).encode()
        return str(int(hashlib.shake_128(o).hexdigest(16), 16))