import asyncio
import logging
from collections.abc import Sequence
from contextlib import suppress
from contextvars import Context
from typing import Callable, Dict, Set

import anyio
from aiormq import Connection
from pamqp.commands import Basic

from commandcenter.eventbus.models import Event, TopicSubscription
from commandcenter.eventbus.subscriber import EventSubscriber
from commandcenter.integrations.exceptions import (
    ManagerClosed,
    SubscriptionLimitError,
    SubscriptionTimeout
)
from commandcenter.integrations.protocols import Manager
from commandcenter.util import EqualJitterBackoff



_LOGGER = logging.getLogger("commandcenter.eventbus.manager")


class EventManager(Manager):
    def __init__(
        self,
        factory: Callable[[],"Connection"],
        exchange: str,
        max_subscribers: int = 100,
        max_buffered_events: int = 1000,
        maxlen: int = 100,
        timeout: float = 5,
        max_backoff: float = 5,
        initial_backoff: float = 1, 
    ) -> None:
        
        self._max_subscribers = max_subscribers
        self._maxlen = maxlen
        self._timeout = timeout
        self._exchange = exchange

        self._subscribers: Dict[asyncio.Future, EventSubscriber] = {}
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self._ready: asyncio.Event = asyncio.Event()
        self._publish_queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=max_buffered_events)
        self._deferred: Set[asyncio.Future] = set()
        self._connection: Connection = None

        self._runner: asyncio.Task = Context().run(
            self._loop.create_task,
            self._run(
                factory,
                max_backoff,
                initial_backoff
            )
        )

    @property
    def closed(self) -> bool:
        return self._runner is None or self._runner.done()

    async def close(self) -> None:
        fut, self._runner = self._runner, None
        if fut is not None:
            fut.cancel()
        for fut in self._deferred: fut.cancel()

    async def subscribe(self, subscriptions: Sequence[TopicSubscription]) -> EventSubscriber:
        if self.closed:
            raise ManagerClosed()
        if len(self._subscribers) >= self._max_subscribers:
            raise SubscriptionLimitError(self._max_subscribers)
        
        subscriptions = set(subscriptions)

        connection = await self.wait()

        subscriber = EventSubscriber()
        fut = subscriber.start(subscriptions, self._maxlen, connection, self._exchange)
        fut.add_done_callback(self.subscriber_lost)
        self._subscribers[fut] = subscriber

    async def wait(self) -> Connection:
        """Wait for backend to be ready."""
        if self._ready.is_set():
            return
        waiter = self._loop.create_task(self._ready.wait())
        try:
            await asyncio.wait_for(waiter, timeout=self._timeout)
        except asyncio.TimeoutError as e:
            raise SubscriptionTimeout("Timed out waiting for service to be ready.") from e
        else:
            assert self._connection is not None
            return self._connection

    def publish(self, event: Event) -> bool:
        if self._publish_queue.full():
            return False
        self._publish_queue.put_nowait((1, event))
        return True

    def subscriber_lost(self, fut: asyncio.Future) -> None:
        assert fut in self._subscribers
        subscriber = self._subscribers.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        e = e or subscriber.exception
        if e is not None:
            _LOGGER.warning("Unhandled error in %s", subscriber.__class__.__name__, exc_info=e)

    async def _publish_events(self, connection: Connection) -> None:
        exchange = self._exchange
        channel = await connection.channel(publisher_confirms=True)
        await channel.exchange_declare(exchange, exchange_type="topic")
        _LOGGER.debug("%i buffered events", self._publish_queue.qsize())
        while True:
            event = await self._publish_queue.get()
            routing_key, payload = event.publish()
            confirmation = await channel.basic_publish(
                payload,
                exchange=exchange,
                routing_key=routing_key
            )
            match confirmation:
                case Basic.Ack():
                    pass
                case Basic.Nack():
                    fut = self._loop.create_task(self._publish_queue.put((0, event)))
                    fut.add_done_callback(self._deferred.discard)
                    self._deferred.add(fut)

    async def _wait_for_connection_lost(self, closer: asyncio.Future) -> None:
        # When there are no events queued up, the manager will hang even though
        # the rabbit mq connection is lost because it will infinitely wait for
        # data in `_publish_events`. The connection lost never gets propagated.
        # This task simply awaits the closing future of the connection
        await closer

    async def _run(
        self,
        factory: Callable[[],"Connection"],
        max_backoff: float,
        initial_backoff: float,
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
                continue
            
            else:
                attempts = 0
                self._connection = connection
                self._ready.set()
                _LOGGER.debug("Connection established")
            
            try:
                async with anyio.create_task_group() as tg:
                    tg.start_soon(self._publish_events, connection)
                    tg.start_soon(self._wait_for_connection_lost, connection.closing)
            except (Exception, anyio.ExceptionGroup):
                self._connection = None
                for fut in self._subscribers.keys():
                    if not fut.done():
                        fut.cancel()

                self._ready.clear()
                sleep = backoff.compute(0)

                _LOGGER.warning(
                    "Manager unavailable, attempting to reconnect in %0.2f seconds",
                    sleep,
                    exc_info=True
                )
                await asyncio.sleep(sleep)
            finally:
                self._connection = None
                for fut in self._subscribers.keys():
                    if not fut.done():
                        fut.cancel()
                await connection.close()