import asyncio
import logging
from contextvars import Context
from typing import Callable, Type

import anyio
from anyio.abc import TaskStatus
try:
    from aiormq import Connection
    from aiormq.abc import DeliveredMessage
except ImportError:
    pass

from commandcenter.integrations.base import BaseLock, BaseDistributedManager
from commandcenter.integrations.exceptions import ClientClosed
from commandcenter.integrations.protocols import Client, Subscriber
from commandcenter.util import EqualJitterBackoff



_LOGGER = logging.getLogger("commandcenter.integrations.managers")


class RabbitMQManager(BaseDistributedManager):
    """A manager for distributed environments backed by RabbitMQ.
    
    Args:
        client: The client instance which connects to and streams data from the
            data source.
        subscriber: The subscriber type to use for this manager.
        lock: The subscription lock.
        factory: A callable which produce an `aiormq.Connection`.
        exchange: The exchange name to use.
        max_subscribers: The maximum number of concurrent subscribers which can
            run by a single manager. If the limit is reached, the manager will
            refuse the attempt and raise a `SubscriptionLimitError`.
        maxlen: The maximum number of messages that can buffered on the subscriber.
            If the buffer limit on the subscriber is reached, the oldest messages
            will be evicted as new messages are added.
        timeout: The time to wait for the backing service to be ready before
            rejecting the subscription request.
        max_backoff: The maximum backoff time in seconds to wait before trying
            to reconnect to the broker.
        initial_backoff: The minimum amount of time in seconds to wait before
            trying to reconnect to the broker.
        max_failed: The maximum number of failed connection attempts before all
            subscribers and client subscriptions tied to this manager are dropped.
    """
    def __init__(
        self,
        client: Client,
        subscriber: Type[Subscriber],
        lock: BaseLock,
        factory: Callable[[],"Connection"],
        exchange: str,
        max_subscribers: int = 100,
        maxlen: int = 100,
        timeout: float = 5,
        max_backoff: float = 5,
        initial_backoff: float = 1,
        max_failed: int = 15
    ) -> None:
        super().__init__(client, subscriber, lock, max_subscribers, maxlen, timeout)
        
        runner: asyncio.Task = Context().run(
            self._loop.create_task,
            self._run(
                factory=factory,
                exchange=exchange,
                max_backoff=max_backoff,
                initial_backoff=initial_backoff,
                max_failed=max_failed
            )
        )
        runner.add_done_callback(lambda _: self._loop.create_task(self.close()))
        self._runner = runner

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
                tg.start_soon(self._manage_subscriptions)
                tg.start_soon(
                    self._manage_broker_connection,
                    factory,
                    exchange,
                    max_backoff,
                    initial_backoff,
                    max_failed
                )
        except (Exception, anyio.ExceptionGroup):
            _LOGGER.error("Manager failed", exc_info=True)
            raise

    async def _manage_broker_connection(
        self,
        factory: Callable[[],"Connection"],
        exchange: str,
        max_backoff: float,
        initial_backoff: float,
        max_failed: int
    ) -> None:
        """Manages broker connection for manager."""
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
                
                if attempts >= max_failed:
                    _LOGGER.error(
                        "Dropping subscribers and client subscriptions due to "
                        "repeated connection failures",
                        len(self._subscribers)
                    )
                    if self.subscriptions:
                        for fut in self._subscribers.keys():
                            if not fut.done():
                                fut.cancel()
                    
                    fut = self._loop.create_task(self._client.unsubscribe(self._client.subscriptions))
                    fut.add_done_callback(lambda _: self._client.clear())
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
                    tg.start_soon(lambda: connection.closing)
            except ClientClosed:
                raise
            except (Exception, anyio.ExceptionGroup):
                pass
            finally:
                self._ready.clear()
                if not connection.is_closed:
                    await asyncio.gather(connection.close(timeout=0.5), return_exceptions=True)
            
            sleep = backoff.compute(attempts)
            _LOGGER.warning(
                "Manager unavailable, attempting to reconnect in %0.2f seconds",
                sleep,
                exc_info=True
            )
            await asyncio.sleep(sleep)