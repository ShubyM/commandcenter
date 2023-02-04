import asyncio
import logging
from contextlib import suppress
from contextvars import Context
from typing import Type

import anyio
from anyio.abc import TaskStatus
try:
    from redis.asyncio import Redis
    from redis.asyncio.client import PubSub
    from redis.exceptions import RedisError
except ImportError:
    pass

from commandcenter.integrations.base import BaseLock, BaseDistributedManager
from commandcenter.integrations.exceptions import ClientClosed
from commandcenter.integrations.protocols import Client, Subscriber
from commandcenter.util import EqualJitterBackoff



_LOGGER = logging.getLogger("commandcenter.integrations.managers")


class RedisManager(BaseDistributedManager):
    """A manager for distributed environments backed by Redis.
    
    Args:
        client: The client instance which connects to and streams data from the
            data source.
        subscriber: The subscriber type to use for this manager.
        lock: The subscription lock.
        redis: The redis client.
        channel: The pub sub channel name.
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
        redis: "Redis",
        channel: str,
        max_subscribers: int = 100,
        maxlen: int = 100,
        timeout: float = 5,
        max_backoff: float = 5,
        initial_backoff: float = 1,
        max_failed: int = 15
    ) -> None:
        super().__init__(client, subscriber, lock, max_subscribers, maxlen, timeout)

        runner = Context().run(
            self._loop.create_task,
            self._run(
                redis,
                channel,
                max_backoff,
                initial_backoff,
                max_failed
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
                tg.start_soon(self._manage_subscriptions)
                tg.start_soon(
                    self._manage_redis_connection,
                    redis,
                    channel,
                    max_backoff,
                    initial_backoff,
                    max_failed
                )
        except (Exception, anyio.ExceptionGroup):
            _LOGGER.error("Manager failed", exc_info=True)
            raise

    async def _manage_redis_connection(
        self,
        redis: "Redis",
        channel: str,
        max_backoff: float,
        initial_backoff: float,
        max_failed: int
    ) -> None:
        """Manages Redis connection for manager."""
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
                pubsub = redis.pubsub()
                _LOGGER.debug("Connection established")
            
            try:
                async with anyio.create_task_group() as tg:
                    await tg.start(self._receive_messages, pubsub)
                    tg.start_soon(self._publish_messages, redis, channel)
            except ClientClosed:
                self._ready.clear()
                with suppress(RedisError):
                    await redis.close(close_connection_pool=True)
                raise
            except (RedisError, anyio.ExceptionGroup):
                self._ready.clear()
                with suppress(RedisError):
                    await redis.close(close_connection_pool=True)
            
            sleep = backoff.compute(0)
            _LOGGER.warning(
                "Manager unavailable, attempting to reconnect in %0.2f seconds",
                sleep,
                exc_info=True
            )
            await asyncio.sleep(sleep)