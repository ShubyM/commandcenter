import asyncio
import itertools
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

from commandcenter.events.exceptions import (
    EventBusClosed,
    EventSubscriptionLimitError,
    EventSubscriptionTimeout
)
from commandcenter.events.models import Event, TopicSubscription
from commandcenter.events.subscriber import EventSubscriber
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
        reconnect_timeout: The time to wait for the broker to reconnect before
            dropping subscribers.
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
        reconnect_timeout: float = 30,
        max_backoff: float = 5,
        initial_backoff: float = 1
    ) -> None:
        
        self._max_subscribers = max_subscribers
        self._maxlen = maxlen
        self._timeout = timeout
        self._reconnect_timeout = reconnect_timeout
        self._exchange = exchange

        self._connection: Connection = None
        self._subscribers: Dict[asyncio.Future, EventSubscriber] = {}
        self._publishers: Dict[asyncio.Task, EventSubscriber] = {}
        self._publish_queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=max_buffered_events)
        self._ready: asyncio.Event = asyncio.Event()
        self._background: Set[asyncio.Future] = set()
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

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

    @property
    def subscriptions(self) -> Set[TopicSubscription]:
        subscriptions = set()
        for fut, subscriber in self._subscribers.items():
            if not fut.done():
                subscriptions.update(subscriber.subscriptions)
        return subscriptions

    def close(self) -> None:
        """Close the event bus."""
        fut, self._runner = self._runner, None
        if fut is not None:
            fut.cancel()
        for fut in itertools.chain(self._subscribers.keys(), self._background):
            fut.cancel()
        self.clear()

    def clear(self) -> None:
        """Clear the publish queue."""
        try:
            while True:
                self._publish_queue.get_nowait()
                self._publish_queue.task_done()
        except asyncio.QueueEmpty:
            pass

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
        try:
            self._publish_queue.put_nowait((1, event))
            return True
        except asyncio.QueueFull:
            return False

    async def subscribe(self, subscriptions: Sequence[TopicSubscription]) -> EventSubscriber:
        """Subscribe to a sequence of topics on the event bus.
        
        Args:
            subscriptions: The topics to subscriber to.
        
        Returns:
            subscriber: The event subscriber instance.

        Raises:
            EventBusClosed: The event bus is closed.
            EventBusSubscriptionLimitError: The event bus is full.
            EventBusSubscriptionTimeout: Timed out waiting for broker connection.
        """
        if self.closed:
            raise EventBusClosed()
        if len(self._subscribers) >= self._max_subscribers:
            raise EventSubscriptionLimitError(self._max_subscribers)
        
        subscriptions = set(subscriptions)
        connection = await self._wait_for_connection(self._timeout)
        subscriber = EventSubscriber()
        
        publisher = self._loop.create_task(
            subscriber.connect(
                connection=connection,
                exchange=self._exchange
            )
        )
        publisher.add_done_callback(self._publisher_lost)
        subscriber.set_publisher(publisher)
        self._publishers[publisher] = subscriber

        fut = subscriber.start(
            subscriptions=subscriptions,
            maxlen=self._maxlen
        )
        fut.add_done_callback(self._subscriber_lost)
        self._subscribers[fut] = subscriber

        return subscriber

    def _subscriber_lost(self, fut: asyncio.Future) -> None:
        """Callback after subscribers have stopped."""
        assert fut in self._subscribers
        self._subscribers.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        if e is not None:
            _LOGGER.warning("Error in subscriber", exc_info=e)

    def _publisher_lost(self, fut: asyncio.Future) -> None:
        """Callback after publishers have stopped."""
        assert fut in self._publishers
        subscriber = self._publishers.pop(fut)
        e: Exception = None
        with suppress(asyncio.CancelledError):
            e = fut.exception()
        if e is not None:
            _LOGGER.warning("Error in publisher", exc_info=e)
        if not subscriber.stopped:
            fut = self._loop.create_task(self._reconnect_publisher(subscriber))
            fut.add_done_callback(self._background.discard)
            self._background.add(fut)

    async def _wait_for_connection(self, timeout: float) -> "Connection":
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=timeout)
        except asyncio.TimeoutError as e:
            raise EventSubscriptionTimeout("Timed out waiting for broker connection.") from e
        else:
            assert self._connection is not None and self._connection.is_opened
            return self._connection

    async def _reconnect_publisher(self, subscriber: EventSubscriber) -> None:
        try:
            connection = await self._wait_for_connection(self._reconnect_timeout)
        except EventSubscriptionTimeout as e:
            subscriber.stop(e)
        else:
            if not subscriber.stopped:
                publisher = self._loop.create_task(
                    subscriber.connect(
                        connection=connection,
                        exchange=self._exchange
                    )
                )
                publisher.add_done_callback(self._publisher_lost)
                subscriber.set_publisher(publisher)
                self._publishers[publisher] = subscriber

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
                    fut.add_done_callback(self._background.discard)
                    self._background.add(fut)

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
                self._connection = None
                for fut in self._publishers.keys(): fut.cancel()
                with suppress(Exception):
                    await connection.close(timeout=2)
                
            sleep = backoff.compute(attempts)
            _LOGGER.warning(
                "Event bus unavailable, attempting to reconnect in %0.2f seconds",
                sleep,
                exc_info=True
            )
            await asyncio.sleep(sleep)