import asyncio
import logging
from collections import deque
from collections.abc import AsyncIterable
from datetime import datetime
from types import TracebackType
from typing import Deque, Set, Type

try:
    from aiormq import Connection
    from aiormq.abc import DeliveredMessage
except ImportError:
    pass

from commandcenter.events.models import (
    EventSubscriberInfo,
    EventSubscriberStatus,
    TopicSubscription
)
from commandcenter.integrations.models import SubscriberCodes
from commandcenter.integrations.protocols import Subscriber



_LOGGER = logging.getLogger("commandcenter.eventbus.subscriber")


class EventSubscriber(Subscriber):
    def __init__(self) -> None:
        self._subscriptions = set()
        self._data: Deque[str | bytes] = None
        self._data_waiter: asyncio.Future = None
        self._stop_waiter: asyncio.Future = None
        self._publisher: asyncio.Task = None
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

        self._created = datetime.utcnow()
        self._total_published = 0

    @property
    def data(self) -> Deque[str | bytes]:
        return self._data

    @property
    def info(self) -> EventSubscriberInfo:
        return EventSubscriberInfo(
            name=self.__class__.__name__,
            stopped=self.stopped,
            status=self.status,
            created=self._created,
            uptime=(datetime.utcnow() - self._created).total_seconds(),
            total_published_messages=self._total_published,
            total_subscriptions=len(self.subscriptions)
        )

    @property
    def status(self) -> EventSubscriberStatus:
        if self._publisher is not None and not self._publisher.done():
            return EventSubscriberStatus.CONNECTED
        return EventSubscriberStatus.DISCONNECTED

    @property
    def stopped(self) -> bool:
        return self._stop_waiter is None or self._stop_waiter.done()

    @property
    def subscriptions(self) -> Set[TopicSubscription]:
        return self._subscriptions

    def set_publisher(self, publisher: asyncio.Task) -> None:
        assert self._publisher is None or self._publisher.done()
        self._publisher = publisher

    def stop(self, e: Exception | None) -> None:
        waiter, self._stop_waiter = self._stop_waiter, None
        if waiter is not None and not waiter.done():
            _LOGGER.debug("%s stopped", self.__class__.__name__)
            if e is not None:
                waiter.set_exception(e)
            else:
                waiter.set_result(None)
        fut, self._publisher = self._publisher, None
        if fut is not None and not fut.done():
            _LOGGER.debug("%s stopping publisher", self.__class__.__name__)
            fut.cancel()

    def publish(self, data: bytes) -> None:
        assert self._data is not None
        self._data.append(data.decode())
        
        waiter, self._data_waiter = self._data_waiter, None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        
        self._total_published += 1
        _LOGGER.debug("Message published to %s", self.__class__.__name__)
    
    def start(self, subscriptions: Set[TopicSubscription], maxlen: int) -> asyncio.Future:
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

    async def connect(self, connection: "Connection", exchange: str) -> None:
        async def on_message(message: DeliveredMessage) -> None:
            data = message.body
            self.publish(data)
        
        channel = await connection.channel(publisher_confirms=False)
        try:
            await channel.exchange_declare(exchange=exchange, exchange_type="topic")
            declare_ok = await channel.queue_declare(exclusive=True)
            
            binds = [
                channel.queue_bind(declare_ok.queue, exchange, topic.routing_key)
                for topic in self._subscriptions
            ]
            await asyncio.gather(*binds)

            await channel.basic_consume(declare_ok.queue, on_message, no_ack=True)
            await asyncio.wait([channel.closing])
        finally:
            if not channel.is_closed:
                await channel.close()

    async def __aiter__(self) -> AsyncIterable[str]:
        if self.stopped:
            return
        while not self.stopped:
            if not self._data:
                code = await self.wait()
                if code is SubscriberCodes.STOPPED:
                    return
            # Pop messages from the data queue until there are no messages
            # left
            while True:
                try:
                    yield self._data.popleft()
                except IndexError:
                    # Empty queue
                    break

    def __enter__(self) -> "EventSubscriber":
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