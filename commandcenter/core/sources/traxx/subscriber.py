import asyncio
import logging
from collections.abc import AsyncIterable
from typing import Callable, Set

from pydantic import ValidationError

from commandcenter.core.integrations.abc import AbstractSubscriber
from commandcenter.core.sources.traxx.models import (
    TraxxSubscriberMessage,
    TraxxSubscription
)



_LOGGER = logging.getLogger("commandcenter.core.sources.traxx")


class TraxxSubscriber(AbstractSubscriber):
    """Subscriber implementation for the Traxx data source."""
    def __init__(
        self,
        subscriptions: Set[TraxxSubscription],
        callback: Callable[["TraxxSubscriber"], None],
        maxlen: int,
    ) -> None:
        super().__init__(subscriptions, callback, maxlen)
        self.data_waiter: asyncio.Future = None
        self.sensors = [subscription.key() for subscription in subscriptions]

    def publish(self, data: str) -> None:
        """Publish data to the subscriber. This method should only be called by
        the manager.
        """
        try:
            data = TraxxSubscriberMessage.parse_raw(data)
        except ValidationError:
            _LOGGER.error("Message validation failed", exc_info=True, extra={"raw": data})

        self.data_queue.append(data)
        
        waiter = self.data_waiter
        self.data_waiter = None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

        _LOGGER.debug("Message published to subscriber")
    
    async def __aiter__(self) -> AsyncIterable[TraxxSubscriberMessage]:
        """Async iterable for streaming real time Traxx data.
        
        This method is intended to be used in event sourcing and websocket contexts.
        The generator will stream data indefinitely until shutdown by the caller
        or stopped by the stream manager due to a subscription issue in the underlying
        client.
        Yields:
            data: A BaseModel containing all the data updates for a single sensor.
        """
        # If `False`, `stop` called before caller could begin iterating
        if await self.start():
            stop = self.stop_waiter
            while not stop.done():
                if not self.data_queue:
                    waiter = self.loop.create_future()
                    self.data_waiter = waiter

                    await asyncio.wait([waiter, stop], return_when=asyncio.FIRST_COMPLETED)
                    if not waiter.done(): # `stop` called waiting for data
                        _LOGGER.debug("Subscriber stopped while waiting for data")
                        waiter.cancel()
                        self.data_waiter = None
                        break

                # Pop messages from the data queue until there are no messages
                # left
                while True:
                    try:
                        msg: TraxxSubscriberMessage = self.data_queue.popleft()
                    except IndexError:
                        # Empty queue
                        break
                    key = f"{msg.sensor_id}-{msg.asset_id}"
                    if key in self.sensors:
                        # The traxx messages are guarenteed to be in monotonically
                        # increasingly order so we dont need to sort or filter
                        # the data here
                        yield msg.json()
                    else:
                        _LOGGER.debug("Message not published %s not in %s", key, ", ".join(self.sensors))