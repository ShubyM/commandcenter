import asyncio
import logging
from collections import deque
from collections.abc import AsyncIterable
from types import TracebackType
from typing import Deque, Set, Type

from aiormq import Connection
from aiormq.abc import DeliveredMessage

from commandcenter.eventbus.models import TopicSubscription
from commandcenter.integrations.models import SubscriberCodes



_LOGGER = logging.getLogger("commandcenter.eventbus.subscriber")


class EventSubscriber:
    def __init__(self) -> None:
        self._subscriptions: Set[TopicSubscription] = set()
        self._data: Deque[bytes] = None
        self._data_waiter: asyncio.Future = None
        self._publisher: asyncio.Future = None
        self._started: asyncio.Future = None
        self._exception: Exception | None = None
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    @property
    def data(self) -> Deque[str]:
        return self._data

    @property
    def stopped(self) -> bool:
        return self._publisher is None or self._publisher.done()

    @property
    def subscriptions(self) -> Set[TopicSubscription]:
        return self._subscriptions

    @property
    def exception(self) -> Exception | None:
        return self._exception

    def stop(self, e: Exception | None) -> None:
        fut, self._publisher = self._publisher, None
        if fut is not None and not fut.done():
            _LOGGER.debug("%s stopped", self.__class__.__name__)
            fut.cancel()
            self._data.clear()
            self._exception = e

    def publish(self, data: bytes) -> None:
        assert self._data is not None
        self._data.append(data)
        
        waiter, self._data_waiter = self._data_waiter, None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        
        _LOGGER.debug("Event published to %s", self.__class__.__name__)

    def start(
        self,
        subscriptions: Set[TopicSubscription],
        maxlen: int,
        connection: Connection,
        exchange: str
    ) -> asyncio.Future:
        assert not self._data
        assert self._publisher is None
        assert self._started is None
        
        self._subscriptions.update(subscriptions)
        self._data = deque(maxlen=maxlen)

        started = self._loop.create_future()
        self._started = started
        publisher = self._loop.create_task(self._publish(connection, exchange))
        self._publisher = publisher
        
        return publisher

    async def wait(self) -> None:
        if self._data_waiter is not None:
            raise RuntimeError("Two coroutines cannot wait for data simultaneously.")
        
        if self.stopped:
            return SubscriberCodes.STOPPED
        
        stop = self._publisher
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

    async def _publish(self, connection: Connection, exchange: str) -> None:
        async def on_message(message: DeliveredMessage) -> None:
            data = message.body
            self.publish(data)
        
        started, self._started = self._started, None
        assert started is not None and not started.done()
        try:
            channel = await connection.channel(publisher_confirms=False)
            try:
                await channel.exchange_declare(exchange=exchange, exchange_type="topic")
                declare_ok = await channel.queue_declare(exclusive=True)
                
                binds = [
                    channel.queue_bind(declare_ok.queue, exchange, topic.routing_key)
                    for topic in self._subscriptions
                ]
                await asyncio.gather(*binds)
                
                started.set_result(None)

                await channel.basic_consume(declare_ok.queue, on_message, no_ack=True)
            finally:
                if not channel.is_closed:
                    await channel.close()
        finally:
            if not started.done():
                started.cancel()

    async def __aiter__(self) -> AsyncIterable[bytes]:
        if self.stopped:
            return
        started = self._started
        if started is not None and not started.done():
            await asyncio.wait([started])
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