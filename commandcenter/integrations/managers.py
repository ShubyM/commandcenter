import asyncio
import logging
import random
from collections.abc import Sequence
from contextvars import Context
from typing import Callable, Set, Type

import anyio
from anyio.abc import TaskStatus
try:
    from redis.asyncio import Redis
    from redis.asyncio.client import PubSub
    from redis.exceptions import RedisError
except ImportError:
    pass
try:
    from aiormq import Connection
    from aiormq.abc import DeliveredMessage
    from aiormq.exceptions import AMQPError
    from pamqp.exceptions import PAMQPException
except ImportError:
    pass

from commandcenter.integrations.base import BaseLock, BaseManager
from commandcenter.integrations.exceptions import (
    ClientClosed,
    ClientSubscriptionError,
    ManagerClosed,
    SubscriptionError,
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

    @property
    def closed(self) -> bool:
        return self._runner is None or self._runner.done()

    async def close(self) -> None:
        fut = self._runner
        self._runner = None
        if fut is not None:
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
                        if msg.error:
                            _LOGGER.warning(
                                "Subscriber dropped due to client connection error",
                                exc_info=msg.error
                            )
                        else:
                            _LOGGER.warning("Subscriber dropped")

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
                tg.start_soon(self._dropped)
                tg.start_soon(self._messages)
                tg.start_soon(self._poll)
        except ClientClosed:
            self._loop.create_task(self.close())
            raise
        except Exception:
            _LOGGER.error("Unhandled error in manager", exc_info=True)
            raise


class _DistributedManager(BaseManager):
    def __init__(
        self,
        client: Client,
        subscriber: Type[Subscriber],
        lock: BaseLock,
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

        await self.wait()
        
        # There is no harm in registering the subscriptions, if `subscribe` fails
        # the locks wont be extended and clients in other processes will eventually
        # unsubscribe once the regstered subscription expires
        await self._lock.register(subscriptions)
        
        await self._subscribe(subscriptions)

        subscriber = self._subscriber()
        fut = subscriber.start(subscriptions, self._maxlen)
        fut.add_done_callback(self.subscriber_lost)
        self._subscribers[fut] = subscriber

        _LOGGER.debug("Added subscriber %i of %i", len(self._subscribers), self._max_subscribers)
        return subscriber

    async def _subscribe(self, subscriptions: Set[BaseSubscription]) -> None:
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
                self._loop.create_task(self.close())
                raise ManagerClosed() from e

            if not subscribed:
                await self._lock.release(to_subscribe)
                raise ClientSubscriptionError("Client refused subscriptions.")

    async def wait(self) -> None:
        """Wait for backend to be ready."""
        if self._ready.is_set():
            return
        waiter = self._loop.create_task(self._ready.wait())
        try:
            await asyncio.wait_for(waiter, timeout=self._timeout)
        except asyncio.TimeoutError as e:
            raise SubscriptionTimeout("Timed out waiting for service to be ready.") from e

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
                # If we cant release a lock the item will expire eventually but
                # there is a chance another subscriber may try and subscribe in
                # a different process, see that a lock is already acquired in
                # this process and assume it is being streamed elsewhere. This
                # will ultimately lead to a dropped subscriber or the other process
                # will actually subscribe on its client because the client
                # subscription lock will not be extended by this process. So, all
                # that is to say, we dont need to raise anything here.
                await self._lock.release(subscriptions)

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
        """Poll client subscriptions owned by this process."""
        while True:
            sleep = (self._lock.ttl + random.randint(0, self._lock.ttl*1000//2))/1000
            await asyncio.sleep(sleep)
            subscriptions = self._client.subscriptions
            if subscriptions:
                unsubscribe = await self._lock.client_poll(subscriptions)
                if unsubscribe:
                    _LOGGER.info("Unsubscribing from %i subscriptions", len(unsubscribe))
                    # We are okay running this in the background, even if the
                    # unsubscribe operation fails, the manager will continue to
                    # extend the client subscriptions as if it still owns the locks.
                    # Between the time when this process first releases the
                    # lock and then re-extends it (affectively re-acquiring it)
                    # another process may acquire the lock. This would
                    # lead to duplicate subscriptions in separate processes,
                    # something we are trying to avoid. However, subscribers
                    # filter out duplicate data so all we are wasting is the
                    # extra resources to stream the same subscription. Not ideal
                    # but we'll live with it.
                    fut = self._loop.create_task(self._client.unsubscribe(unsubscribe))
                    fut.add_done_callback(self._background.discard)
                    self._background.add(fut)
    
    async def _subscriber_poll(self) -> None:
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

    async def _run_subscriptions(self) -> None:
        async with anyio.create_task_group() as tg:
            tg.start_soon(self._dropped)
            tg.start_soon(self._extend_client)
            tg.start_soon(self._extend_subscriber)
            tg.start_soon(self._client_poll)
            tg.start_soon(self._subscriber_poll)


class RabbitMQManager(_DistributedManager):
    """A manager for distributed environments backed by RabbitMQ."""
    def __init__(
        self,
        client: Client,
        subscriber: Type[Subscriber],
        lock: BaseLock,
        factory: Callable[[],"Connection"],
        channel_name: str,
        max_subscribers: int = 100,
        maxlen: int = 100,
        timeout: float = 5,
        max_backoff: float = 5,
        initial_backoff: float = 1,
        max_failed: int = 15
    ) -> None:
        super().__init__(client, subscriber, lock, max_subscribers, maxlen, timeout)
        self._runner: asyncio.Task = Context().run(
            self._loop.create_task,
            self._run(
                factory,
                channel_name,
                max_backoff,
                initial_backoff,
                max_failed
            )
        )

    @property
    def closed(self) -> bool:
        return self._runner is None or self._runner.done()

    async def close(self) -> None:
        fut = self._runner
        self._runner = None
        if fut is not None:
            fut.cancel()
        for fut in self._background: fut.cancel()
        await super().close()
    
    async def _publish_messages(self, connection: "Connection", exchange: str) -> None:
        """Retrieve messages from the client and publish them to the exchange."""
        channel = await connection.channel(publisher_confirms=False)
        await channel.exchange_declare(exchange=exchange, exchange_type="fanout")
        _LOGGER.debug("%i buffered messages on %s", self._client.buffer, self._client.__class__.__name__)
        async for msg in self._client.messages():
            await channel.basic_publish(msg.encode(), exchange=exchange)

    async def _receive_messages(
        self,
        connection: "Connection",
        exchange: str,
        task_status: TaskStatus = anyio.TASK_STATUS_IGNORED
    ) -> None:
        """Retrieve messages from the exchange and publish them to subscribers."""
        channel = await connection.channel(publisher_confirms=False)
        await channel.exchange_declare(exchange=exchange, exchange_type="fanout")
        declare_ok = await channel.queue_declare(exclusive=True)
        await channel.queue_bind(declare_ok.queue, exchange)
        
        async def on_message(message: DeliveredMessage) -> None:
            data = message.body
            for fut, subscriber in self._subscribers.items():
                if not fut.done():
                    subscriber.publish(data)
        
        task_status.started()
        await channel.basic_consume(declare_ok.queue, on_message, no_ack=True)

    async def _wait_for_connection_lost(self, closer: asyncio.Future) -> None:
        # When there are no client subscriptions and no data is coming through,
        # the manager will hang even though the rabbit mq connection is lost
        # because it will infinitely wait for data in `_publish_message`. The
        # connection lost never gets propagated. This task simply awaits the
        # closing future of the connection
        await closer

    async def _run(
        self,
        factory: Callable[[],"Connection"],
        exchange: str,
        max_backoff: float,
        initial_backoff: float,
        max_failed: int
    ) -> None:
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self._run_subscriptions)
                tg.start_soon(
                    self._run_connection,
                    factory,
                    exchange,
                    max_backoff,
                    initial_backoff,
                    max_failed
                )
        except ClientClosed:
            self._loop.create_task(self.close())
            raise
        except ManagerClosed:
            raise
        except (Exception, anyio.ExceptionGroup):
            _LOGGER.error("Unhandled exception in manager", exc_info=True)
            self._loop.create_task(self.close())
            raise

    async def _run_connection(
        self,
        factory: Callable[[],"Connection"],
        exchange: str,
        max_backoff: float,
        initial_backoff: float,
        max_failed: int
    ) -> None:
        """Manages background tasks on manager."""
        attempts = 0
        backoff = EqualJitterBackoff(max_backoff, initial_backoff)
        while True:
            connection = factory()
            _LOGGER.debug("Connecting to %s", connection.url)
            
            try:
                await connection.connect()
            except Exception:
                sleep = backoff.compute(attempts)
                _LOGGER.warning("Connection failed, trying again in %0.2f", sleep, exc_info=True)
                await asyncio.sleep(sleep)
                attempts += 1
                # Up to this point, the client connections are still running
                # so we are still buffering data that can be processed once
                # the connection is back up and running. If we reach the max
                # failed attempts though we drop all subscribers and unsubscribe
                # from all subscriptions. This will allow another process to
                # potentially pick up those subscribers and subscriptions.
                if attempts >= max_failed:
                    subscriptions = self.subscriptions
                    if subscriptions:
                        _LOGGER.warning(
                            "Dropping %i subscribers due to repeated connection failures",
                            len(self._subscribers)
                        )
                        for fut in self._subscribers.keys():
                            if not fut.done():
                                fut.cancel()
                    
                    fut = self._loop.create_task(self._client.unsubscribe(self._client.subscriptions))
                    fut.add_done_callback(self._background.discard)
                    self._background.add(fut)
                
                continue
            
            else:
                attempts = 0
                self._ready.set()
                _LOGGER.debug("Connection established")
            
            try:
                async with anyio.create_task_group() as tg:
                    await tg.start(self._receive_messages, connection, exchange)
                    tg.start_soon(self._publish_messages, connection, exchange)
                    tg.start_soon(self._wait_for_connection_lost, connection.closing)
            except (Exception, anyio.ExceptionGroup):
                self._ready.clear()
                sleep = backoff.compute(0)
                
                _LOGGER.warning(
                    "Manager unavailable, attempting to reconnect in %0.2f seconds",
                    sleep,
                    exc_info=True
                )
                await asyncio.sleep(sleep)
            finally:
                await connection.close()


class RedisManager(_DistributedManager):
    """A manager for distributed environments backed by Redis."""
    def __init__(
        self,
        client: Client,
        subscriber: Type[Subscriber],
        lock: BaseLock,
        redis: "Redis",
        channel_name: str,
        max_subscribers: int = 100,
        maxlen: int = 100,
        timeout: float = 5,
        max_backoff: float = 5,
        initial_backoff: float = 1,
        max_failed: int = 15
    ) -> None:
        super().__init__(client, subscriber, lock, max_subscribers, maxlen, timeout)

        self._runner: asyncio.Task = Context().run(
            self._loop.create_task,
            self._run(
                redis,
                channel_name,
                max_backoff,
                initial_backoff,
                max_failed
            )
        )

    @property
    def closed(self) -> bool:
        return self._runner is None or self._runner.done()

    async def close(self) -> None:
        fut = self._runner
        self._runner = None
        if fut is not None:
            fut.cancel()
        for fut in self._background: fut.cancel()
        await super().close()
    
    async def _publish_messages(self, redis: "Redis", channel: str) -> None:
        """Retrieve messages from the client and publish them to the channel."""
        async for msg in self._client.messages():
            await redis.publish(channel, msg.encode())

    async def _receive_messages(
        self,
        pubsub: "PubSub",
        task_status: TaskStatus = anyio.TASK_STATUS_IGNORED
    ) -> None:
        """Retrieve messages from the channel and publish them to subscribers."""
        while True:
            task_status.started()
            msg = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=10_000_000_000
            )
            if msg is not None:
                data = msg["data"]
                for fut, subscriber in self._subscribers.items():
                    if not fut.done():
                        subscriber.publish(data)

    async def _run(
        self,
        redis: "Redis",
        channel: str,
        max_backoff: float,
        initial_backoff: float,
        max_failed: int
    ) -> None:
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self._run_subscriptions)
                tg.start_soon(
                    self._run_connection,
                    redis,
                    channel,
                    max_backoff,
                    initial_backoff,
                    max_failed
                )
        except ClientClosed:
            self._loop.create_task(self.close())
            raise
        except ManagerClosed:
            raise
        except (Exception, anyio.ExceptionGroup):
            _LOGGER.error("Unhandled exception in manager", exc_info=True)
            self._loop.create_task(self.close())
            raise

    async def _run_connection(
        self,
        redis: "Redis",
        channel: str,
        max_backoff: float,
        initial_backoff: float,
        max_failed: int
    ) -> None:
        """Manages background tasks on manager."""
        attempts = 0
        backoff = EqualJitterBackoff(max_backoff, initial_backoff)
        pieces = redis.connection_pool.connection_class(
            **redis.connection_pool.connection_kwargs
        ).repr_pieces()
        url_args = ",".join((f"{k}={v}" for k, v in pieces))

        while True:
            _LOGGER.debug("Pinging redis %s", url_args)
            
            try:
                await redis.ping()
            except RedisError:
                sleep = backoff.compute(attempts)
                _LOGGER.warning("Connection failed, trying again in %0.2f", sleep, exc_info=True)
                await asyncio.sleep(sleep)
                attempts += 1
                
                # See comment in RabbitMQ manager
                if attempts >= max_failed:
                    subscriptions = self.subscriptions
                    if subscriptions:
                        _LOGGER.warning(
                            "Dropping %i subscribers due to repeated connection failures",
                            len(self._subscribers)
                        )
                        for fut in self._subscribers.keys():
                            if not fut.done():
                                fut.cancel()
                    
                    fut = self._loop.create_task(self._client.unsubscribe(self._client.subscriptions))
                    fut.add_done_callback(self._background.discard)
                    self._background.add(fut)
                
                continue
            
            else:
                attempts = 0
                self._ready.set()
                pubsub = redis.pubsub()
                _LOGGER.debug("Connection established")
            
            try:
                async with anyio.create_task_group() as tg:
                    await tg.start(self._receive_messages, pubsub)
                    tg.start_soon(self._publish_messages, redis, channel)
            except (Exception, anyio.ExceptionGroup):
                self._ready.clear()
                sleep = backoff.compute(0)
                
                _LOGGER.warning(
                    "Manager unavailable, attempting to reconnect in %0.2f seconds",
                    sleep,
                    exc_info=True
                )
                await asyncio.sleep(sleep)
            finally:
                await redis.close()


class Managers(ObjSelection):
    DEFAULT = "default", LocalManager
    LOCAL = "local", LocalManager
    RABBITMQ = "rabbitmq", RabbitMQManager
    REDIS = "redis", RedisManager