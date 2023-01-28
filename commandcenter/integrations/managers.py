import asyncio
import logging
import random
from collections.abc import Sequence
from typing import Set, Type

import anyio
try:
    from redis.asyncio import Redis
    from redis.exceptions import RedisError
except ImportError:
    pass
try:
    from aiormq import Connection
    from aiormq.abc import DeliveredMessage
    from aiormq.exceptions import AMQPError
except ImportError:
    pass

from commandcenter.integrations.base import BaseLock, BaseManager
from commandcenter.integrations.exceptions import (
    ClientClosed,
    ClientSubscriptionError,
    IntegrationError,
    LockingError,
    ManagerClosed,
    SubscriptionLimitError,
    SubscriptionLockError,
    SubscriptionTimeout
)
from commandcenter.integrations.models import BaseSubscription
from commandcenter.integrations.protocols import Client, Subscriber
from commandcenter.util import EqualJitterBackoff, ObjSelection



_LOGGER = logging.getLogger("commandcenter.integrations.managers")


class LocalManager(BaseManager):
    """A manager for non-distributed environments."""
    def __init__(
        self,
        client: Client,
        subscriber: Type[Subscriber],
        max_subscribers: int = 100,
        maxlen: int = 100
    ) -> None:
        super().__init__(client, subscriber, max_subscribers, maxlen)
        
        self._runner: asyncio.Task = self._loop.create_task(self._run())
        self._background: Set[asyncio.Task] = set()

    @property
    def closed(self) -> bool:
        return self._runner is None or self._runner.done()

    async def close(self) -> None:
        fut = self._runner
        self._runner = None
        fut.cancel()
        for fut in self._background: fut.cancel()
        await super().close()

    async def subscribe(
        self,
        subscriptions: Sequence[BaseSubscription]
    ) -> Subscriber:
        """Subscribe on the client and configure a subscriber.
        
        Args:
            subscriptions: The subscriptions to subscribe to.
        
        Returns:
            subscriber: The configured subscriber.
        
        Raises:
            ClientSubscriptionError: Unable to subscribe on the client.
            ManagerClosed: Cannot subscribe, manager is closed.
            SubscriptionLimitError: Max number of subscribers reached.
        """
        if self.closed:
            raise ManagerClosed()
        if len(self._subscribers) >= self._max_subscribers:
            raise SubscriptionLimitError(self._max_subscribers)
        
        subscriptions = set(subscriptions)
        
        try:
            subscribed = await self._client.subscribe(subscriptions)
        except ClientClosed as e:
            await self.close()
            raise ManagerClosed() from e

        if not subscribed:
            raise ClientSubscriptionError("Client refused subscriptions.")

        subscriber = self._subscriber()
        fut = subscriber.start(subscriptions, self._maxlen)
        fut.add_done_callback(self.subscriber_lost)
        self._subscribers[fut] = subscriber

        _LOGGER.debug("Added subscriber %i of %i", len(self._subscribers), self._max_subscribers)
        return subscriber

    def subscriber_lost(self, fut: asyncio.Future) -> None:
        super().subscriber_lost(fut)
        self._event.set()

    async def _messages(self) -> None:
        """Retrieve messages from the client and publish them to subscribers."""
        async for msg in self._client.messages():
            for fut, subscriber in self._subscribers.items():
                if not fut.done():
                    subscriber.publish(msg)

    async def _dropped(self) -> None:
        """Retrieve dropped subscriptions and stop any dependent subscribers."""
        async for msg in self._client.dropped():
            subscriptions = msg.subscriptions
            if subscriptions:
                for fut, subscriber in self._subscribers.items():
                    if (
                        not fut.done() and
                        subscriptions.difference(subscriber.subscriptions) != subscriptions
                    ):
                        fut.cancel()
                        # There should always be an error associated if we get
                        # here. If there isnt thats a bug in the client implementation.
                        assert msg.error is not None
                        _LOGGER.warning(
                            "Subscriber dropped due to client connection error",
                            exc_info=msg.error
                        )

    async def _poll(self) -> None:
        """Poll subscribers and cross reference with any unusued client
        subscriptions.

        Unsubscribe from any client subscriptions which are no longer required.
        """
        while True:
            await self._event.wait()
            self._event.clear()
            # If a subscriber disconnects then reconnects we dont need to close
            # the client connections. So we give it a little time.
            await asyncio.sleep(5)
            subscriptions = self.subscriptions
            unsubscribe = self._client.subscriptions.difference(subscriptions)
            if unsubscribe:
                _LOGGER.debug("Unsubscribing from %i subscriptions", len(unsubscribe))
                fut = self._loop.create_task(self._client.unsubscribe(unsubscribe))
                fut.add_done_callback(self._background.discard)
                self._background.add(fut)

    async def _run(self) -> None:
        """Manages background tasks on manager."""
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(LocalManager._dropped, self)
                tg.start_soon(LocalManager._messages, self)
                tg.start_soon(LocalManager._poll, self)
        except ClientClosed:
            self._loop.create_task(self.close())
            raise
        except Exception:
            _LOGGER.error("Unhandled error in manager", exc_info=True)
            raise


class RabbitMQManager(BaseManager):
    """A manager for distributed environments backed by RabbitMQ."""
    def __init__(
        self,
        client: Client,
        connection: "Connection",
        exchange: str,
        lock: BaseLock,
        subscriber: Type[Subscriber],
        max_subscribers: int = 100,
        maxlen: int = 100,
        max_backoff: float = 3,
        max_failed: int = 5,
        initial_backoff: float = 0.5,
        timeout: float = 5
    ) -> None:
        super().__init__(client, subscriber, max_subscribers, maxlen)
        self._lock = lock
        self._timeout = timeout

        self._runner: asyncio.Task = self._loop.create_task(
            self._run(
                connection,
                exchange,
                max_backoff,
                initial_backoff,
                max_failed
            )
        )
        self._background: Set[asyncio.Task] = set()
        self._ready: asyncio.Event = asyncio.Event()

    @property
    def closed(self) -> bool:
        return self._runner is None or self._runner.done()

    async def close(self) -> None:
        fut = self._runner
        self._runner = None
        fut.cancel()
        for fut in self._background: fut.cancel()
        await super().close()

    async def subscribe(
        self,
        subscriptions: Sequence[BaseSubscription]
    ) -> Subscriber:
        """Subscribe on the client and configure a subscriber.
        
        Args:
            subscriptions: The subscriptions to subscribe to.
        
        Returns:
            subscriber: The configured subscriber.
        
        Raises:
            ClientSubscriptionError: Unable to subscribe on the client.
            ManagerClosed: Cannot subscribe, manager is closed.
            SubscriptionLimitError: Max number of subscribers reached.
            SubscriptionTimeout: Timed out waiting for RabbitMQ service to be ready.
            SubscriptionLockError: Unable to acquire locks for subscriptions.
        """
        if self.closed:
            raise ManagerClosed()
        if len(self._subscribers) >= self._max_subscribers:
            raise SubscriptionLimitError(self._max_subscribers)
        
        subscriptions = set(subscriptions)

        waiter = self._loop.create_task(self._ready.wait())
        try:
            await asyncio.wait_for(waiter, timeout=self._timeout)
        except asyncio.TimeoutError as e:
            raise SubscriptionTimeout("Timed out waiting for service to be ready.") from e
        
        try:
            to_subscribe = await self._lock.acquire(subscriptions)
        except LockingError as e:
            raise SubscriptionLockError("Unable to acquire locks") from e

        if to_subscribe:
            try:
                subscribed = await self._client.subscribe(subscriptions)
            except ClientClosed as e:
                await self._lock.release(to_subscribe)
                await self.close()
                raise ManagerClosed() from e

            if not subscribed:
                await self._lock.release(to_subscribe)
                raise ClientSubscriptionError("Client refused subscriptions.")

        await self._lock.register(subscriptions)

        subscriber = self._subscriber()
        fut = subscriber.start(subscriptions, self._maxlen)
        fut.add_done_callback(self.subscriber_lost)
        self._subscribers[fut] = subscriber

        _LOGGER.debug("Added subscriber %i of %i", len(self._subscribers), self._max_subscribers)
        return subscriber

    async def _dropped(self) -> None:
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
    
    async def _publish_messages(self, connection: Connection, exchange: str) -> None:
        """Retrieve messages from the client and publish them to the exchange."""
        channel = await connection.channel(publisher_confirms=False)
        await channel.exchange_declare(exchange=exchange, exchange_type="fanout")
        while True:
            async for msg in self._client.messages():
                await channel.basic_publish(msg.encode(), exchange=exchange)

    async def _receive_messages(self, connection: Connection, exchange: str) -> None:
        """Retrieve messages from the exchange and publish them to subscribers."""
        channel = await connection.channel(publisher_confirms=False)
        await channel.exchange_declare(exchange=exchange, exchange_type="fanout")
        declare_ok = await channel.queue_declare(exclusive=True)
        await channel.queue_bind(declare_ok.queue, exchange)
        
        async def on_message(message: DeliveredMessage) -> None:
            for fut, subscriber in self._subscribers.items():
                if not fut.done():
                    subscriber.publish(message.body)
            await message.channel.basic_ack(message.delivery.delivery_tag)
        
        await channel.basic_consume(declare_ok.queue, on_message)

    async def _extend_client(self) -> None:
        """Extend client locks owned by this process."""
        while True:
            sleep = (self._lock.ttl*1000//2 - random.randint(0, self._lock.ttl*1000//4))/1000
            await asyncio.sleep(sleep)
            subscriptions = self._client.subscriptions
            if subscriptions:
                await self._lock.extend_client(subscriptions)

    async def _extend_subscriber(self) -> None:
        """Extend subscriber registrations owned by this process."""
        while True:
            sleep = (self._lock.ttl*1000//2 - random.randint(0, self._lock.ttl*1000//4))/1000
            await asyncio.sleep(sleep)
            subscriptions = self.subscriptions
            if subscriptions:
                await self._lock.extend_subscriber(subscriptions)

    async def _client_poll(self) -> None:
        while True:
            sleep = (self._lock.ttl + random.randint(0, self._lock.ttl*1000//2))/1000
            await asyncio.sleep(sleep)
            subscriptions = self._client.subscriptions
            if subscriptions:
                unsubscribe = await self._lock.client_poll(subscriptions)
                if unsubscribe:
                    await self._lock.release(unsubscribe)
                    fut = self._loop.create_task(self._client.unsubscribe(unsubscribe))
                    fut.add_done_callback(self._background.discard)
                    self._background.add(fut)
                    _LOGGER.debug("Unsubscribing from %i subscriptions", len(unsubscribe))
    
    async def _subscriber_poll(self) -> None:
        while True:
            sleep = (self._lock.ttl + random.randint(0, self._lock.ttl*1000//2))/1000
            await asyncio.sleep(sleep)
            subscriptions = self.subscriptions
            if subscriptions:
                not_subscribed = await self._lock.subscriber_poll(subscriptions)
                if not_subscribed:
                    try:
                        to_subscribe = await self._lock.acquire(not_subscribed)
                        subscribed = await self._client.subscribe(to_subscribe)
                        if not subscribed:
                            await self._lock.release()
                            raise ClientSubscriptionError("Client refused subscriptions.")
                    except IntegrationError as e:
                        for fut, subscriber in self._subscribers.items():
                            if (
                                not fut.done() and
                                not_subscribed.difference(subscriber.subscriptions) != not_subscribed
                            ):
                                fut.cancel()
                                _LOGGER.warning(
                                    "Subscriber dropped. Unable to pick up lost subscriptions",
                                    exc_info=e
                                )

    async def _run(
        self,
        connection: "Connection",
        exchange: str,
        max_backoff: float,
        initial_backoff: float,
        max_failed: int
    ) -> None:
        attempts = 0
        backoff = EqualJitterBackoff(max_backoff, initial_backoff)
        while True:
            assert connection.is_closed
            _LOGGER.debug("Connecting to %s", connection.url)
            try:
                await connection.connect()
            except AMQPError:
                sleep = backoff.compute(attempts)
                _LOGGER.warning("Connection failed, trying again in %0.2f", sleep, exc_info=True)
                await asyncio.sleep(sleep)
                attempts += 1
                if attempts >= max_failed:
                    subscriptions = self.subscriptions
                    if subscriptions:
                        _LOGGER.warning(
                            "Dropping %i subscribers due to repeated connection failures",
                            len(self._subscribers)
                        )
                        for fut in self._subscribers.keys(): fut.cancel()
                continue
            else:
                attempts = 0
                self._ready.set()
                _LOGGER.debug("Connection established")
            
            try:
                async with anyio.create_task_group() as tg:
                    tg.start_soon(RabbitMQManager._dropped, self)
                    tg.start_soon(RabbitMQManager._client_poll, self)
                    tg.start_soon(RabbitMQManager._subscriber_poll, self)
                    tg.start_soon(RabbitMQManager._extend_client, self)
                    tg.start_soon(RabbitMQManager._extend_subscriber, self)
                    tg.start_soon(RabbitMQManager._publish_messages, self, connection, exchange)
                    tg.start_soon(RabbitMQManager._receive_messages, self, connection, exchange)
            except Exception:
                self._ready.clear()
                sleep = backoff.compute(0)
                if not connection.is_closed:
                    await connection.close()
                _LOGGER.warning(
                    "Manager unavailable, attempting to reconnect in %0.2f seconds",
                    sleep,
                    exc_info=True
                )
                await asyncio.sleep(sleep)


class RedisManager:
    """A manager for distributed environments backed by Redis."""


class Managers(ObjSelection):
    DEFAULT = "default", LocalManager
    LOCAL = "local", LocalManager
    RABBITMQ = "rabbitmq", RabbitMQManager
    REDIS = "redis", RedisManager