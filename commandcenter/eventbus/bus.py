import asyncio
import logging
from collections.abc import Sequence
from contextlib import suppress
from contextvars import Context
from typing import Callable, Dict, Set

import anyio
try:
    from aiormq import Connection
    from pamqp.commands import Basic
except ImportError:
    pass

from commandcenter.eventbus.exceptions import (
    EventBusClosed,
    EventBusSubscriptionLimitError,
    EventBusSubscriptionTimeout
)
from commandcenter.eventbus.models import Event, TopicSubscription
from commandcenter.eventbus.subscriber import EventSubscriber
from commandcenter.util import EqualJitterBackoff



_LOGGER = logging.getLogger("commandcenter.eventbus")


class EventBus:
    """Event bus backed by RabbitMQ topic exchange.
    
    Args:
        factory: A callable which produce an `aiormq.Connection`.
        exchange: The exchange name to use.
        max_subscribers: The maximum number of concurrent subscribers which can
            run by a single bus. If the limit is reached, the bus will
            refuse the attempt and raise a `SubscriptionLimitError`.
        max_buffered_events: The maximum number of events that can be buffered
            on the bus waiting to be published.
        maxlen: The maximum number of messages that can buffered on the subscriber.
            If the buffer limit on the subscriber is reached, the oldest messages
            will be evicted as new messages are added.
        timeout: The time to wait for the broker service to be ready before
            rejecting the subscription request.
        max_backoff: The maximum backoff time in seconds to wait before trying
            to reconnect to the broker.
        initial_backoff: The minimum amount of time in seconds to wait before
            trying to reconnect to the broker.
    """
    def __init__(
        self,
        factory: Callable[[], "Connection"],
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
        self._republish: Set[asyncio.Future] = set()
        self._connection: Connection = None

        runner: asyncio.Task = Context().run(
            self._loop.create_task,
            self._manage_broker_connection(
                factory=factory,
                max_backoff=max_backoff,
                initial_backoff=initial_backoff
            )
        )
        runner.add_done_callback(lambda _: self.close())
        self._runner = runner

    @property
    def closed(self) -> bool:
        return self._runner is None or self._runner.done()

    def close(self) -> None:
        """Close the event bus."""
        fut, self._runner = self._runner, None
        if fut is not None:
            fut.cancel()
        for fut in self._subscribers.keys():
            if not fut.done():
                fut.cancel()
        for fut in self._republish: fut.cancel()
        self.clear()

    def clear(self) -> None:
        """Clear the publish queue."""
        try:
            while True:
                self._publish_queue.get_nowait()
                self._publish_queue.task_done()
        except asyncio.QueueEmpty:
            pass

    async def subscribe(self, subscriptions: Sequence[TopicSubscription]) -> EventSubscriber:
        """Subscribe to a sequence of topics on the event bus.
        
        Args:
            subscriptions: The topics to subscriber to.
        
        Returns:
            subscriber: The event subscriber instance.

        Raises:
            EventBusClosed: The event bus is closed.
            EventBusSubscriptionLimitError: The event bus is full.
            EventBusSubscriptionTimeout: Timed out waiting for broker connection
                to ready.
            EventBusConnectionError: Broker connection expired before subscriber
                could subscribe.
        """
        if self.closed:
            raise EventBusClosed()
        if len(self._subscribers) >= self._max_subscribers:
            raise EventBusSubscriptionLimitError(self._max_subscribers)
        
        subscriptions = set(subscriptions)

        try:
            await asyncio.wait_for(self._ready.wait(), timeout=self._timeout)
        except asyncio.TimeoutError as e:
            raise EventBusSubscriptionTimeout(
                "Timed out waiting for service to be ready."
            ) from e
        else:
            assert self._connection is not None and self._connection.is_opened
            connection = self._connection

        subscriber = EventSubscriber()
        fut = await subscriber.start(
            subscriptions=subscriptions,
            maxlen=self._maxlen,
            connection=connection,
            exchange=self._exchange
        )
        fut.add_done_callback(self.subscriber_lost)
        self._subscribers[fut] = subscriber

    def publish(self, event: Event) -> bool:
        """Publish an event to the bus.
        
        Args:
            event: The event to publish.
        
        Returns:
            bool: If `True` event will be published. If `False` publish queue
                if full, event was not enqueued.
        
        Raises:
            EventBusClosed: 
        """
        if self.closed:
            raise EventBusClosed()
        if self._publish_queue.full():
            return False
        self._publish_queue.put_nowait((1, event))
        return True

    def subscriber_lost(self, fut: asyncio.Future) -> None:
        """Callback after subscribers have stopped."""
        assert fut in self._subscribers
        subscriber = self._subscribers.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        e = e or subscriber.exception
        if e is not None:
            _LOGGER.warning("Error in subscriber", exc_info=e)

    async def _publish_events(self, connection: "Connection") -> None:
        """Publish enqueued events to the broker."""
        exchange = self._exchange
        channel = await connection.channel(publisher_confirms=True)
        await channel.exchange_declare(exchange, exchange_type="topic")
        
        while True:
            event = await self._publish_queue.get()
            self._publish_queue.task_done()
            routing_key, payload = event.publish()
            
            confirmation = await channel.basic_publish(
                payload,
                exchange=exchange,
                routing_key=routing_key
            )
            
            match confirmation:
                case Basic.Ack():
                    continue
                case Basic.Nack():
                    fut = self._loop.create_task(self._publish_queue.put((0, event)))
                    fut.add_done_callback(self._republish.discard)
                    self._republish.add(fut)

    async def _manage_broker_connection(
        self,
        factory: Callable[[],"Connection"],
        max_backoff: float,
        initial_backoff: float
    ) -> None:
        """Manages broker connection for event bus."""
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
                    await tg.start(self._publish_events, connection)
                    tg.start_soon(lambda: connection.closing)
            except (Exception, anyio.ExceptionGroup):
                self._ready.clear()
                with suppress(Exception):
                    await connection.close(timeout=2)
                pass
            finally:
                self._connection = None
                for fut in self._subscribers.keys():
                    if not fut.done():
                        fut.cancel()
            
            sleep = backoff.compute(attempts)
            _LOGGER.warning(
                "Event bus unavailable, attempting to reconnect in %0.2f seconds",
                sleep,
                exc_info=True
            )
            await asyncio.sleep(sleep)