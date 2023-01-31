import logging
from collections.abc import AsyncIterable
from datetime import datetime
from typing import Dict

from pydantic import ValidationError

from commandcenter.integrations.base import BaseSubscriber, SubscriberCodes
from commandcenter.sources.traxx.models import TraxxSubscriberMessage, TraxxSubscription



_LOGGER = logging.getLogger("commandcenter.sources.traxx")


class TraxxSubscriber(BaseSubscriber):
    """Subscriber implementation for the Traxx data source."""
    async def __aiter__(self) -> AsyncIterable[str]:
        """Async iterable for streaming real time Traxx data.

        Yields:
            data: A JSON string containing all the data updates for a single sensor.
        """
        if self.stopped:
            return
        ref: Dict[TraxxSubscription, datetime] = {
            subscription: None for subscription in self._subscriptions
        }
        while not self.stopped:
            if not self._data:
                code = await self.wait()
                if code is SubscriberCodes.STOPPED:
                    return
            # Pop messages from the data queue until there are no messages
            # left
            while True:
                try:
                    msg = self._data.popleft()
                except IndexError:
                    # Empty queue
                    break
                else:
                    try:
                        data = TraxxSubscriberMessage.parse_raw(msg)
                    except ValidationError:
                        _LOGGER.error(
                            "Message validation failed",
                            exc_info=True,
                            extra={"raw": data}
                        )
                        continue
                
                subscription = data.subscription
                if subscription in self._subscriptions:
                    timestamp = data.items[-1].timestamp # Most recent timestamp
                    last = ref.get(subscription)
                    if last is None or last < timestamp:
                        yield data.json()
                    ref[subscription] = last