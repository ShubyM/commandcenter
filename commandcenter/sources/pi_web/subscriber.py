import logging
from collections.abc import AsyncIterable
from datetime import datetime
from typing import Dict

from pydantic import ValidationError

from commandcenter.integrations.base import BaseSubscriber, SubscriberCodes
from commandcenter.sources.pi_web.models import PISubscriberMessage



_LOGGER = logging.getLogger("commandcenter.sources.pi_web")


class PISubscriber(BaseSubscriber):
    """Subscriber implementation for the PI Web API data source."""
    async def __aiter__(self) -> AsyncIterable[str]:
        """Async iterable for streaming real time PI data.

        Yields:
            data: A JSON string containing all the data updates for a single WebId.
        """
        if self.stopped:
            return
        ref: Dict[str, datetime] = {
            subscription.web_id: None for subscription in self._subscriptions
        }
        index = {subscription.web_id: subscription for subscription in self._subscriptions}
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
                        data = PISubscriberMessage.parse_raw(msg)
                    except ValidationError:
                        _LOGGER.error(
                            "Message validation failed",
                            exc_info=True,
                            extra={"raw": data}
                        )
                        continue
                
                for item in data.items:
                    web_id = item.web_id
                    timestamp = item.items[-1].timestamp # Most recent timestamp
                    if web_id in index:
                        # Only yield the data if the timestamp is greater than
                        # the last for this WebId
                        last = ref.get(web_id)
                        if last is None or last < timestamp:
                            subscription = index[web_id]
                            yield item.subscription_message(subscription)
                        ref[web_id] = timestamp