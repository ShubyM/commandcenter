from datetime import datetime, timedelta
from typing import AsyncIterable, Sequence

from commandcenter.integrations.base import BaseCollection
from commandcenter.integrations.models import BaseSubscription
from commandcenter.timeseries import TimeseriesCollection
from commandcenter.types import TimeseriesRow
from commandcenter.util.timeutils import split_range



class LocalCollection(BaseCollection):
    """Timeseries collection using the local backend API.

    Args:
        subscriptions: A sequence of the subscriptions to stream data for. The
            data will be streamed in sorted order of the subscriptions (using
            the hash of the subscription).
        delta: A timedelta for the stream period.
        collection: The backend timeseries collection to iterate from.
    """
    def __init__(
        self,
        subscriptions: Sequence[BaseSubscription],
        delta: timedelta,
        collection: TimeseriesCollection
    ) -> None:
        super().__init__(subscriptions, delta)
        self.collection = collection

    async def __aiter__(self) -> AsyncIterable[TimeseriesRow]:
        waiter = self.close_waiter
        if waiter is None or waiter.done():
            return
        
        end = datetime.now()
        start = end - self.delta
        
        last_timestamp = None
        start_times, end_times = split_range(start, end, timedelta(minutes=60))

        for start_time, end_time in zip(start_times, end_times):
            view = self.collection.filter_by_subscription(self.subscriptions)
            async for i, (timestamp, row) in enumerate(view.iter_range(start_time, end_time)):
                if i == 0 and last_timestamp is not None and last_timestamp >= timestamp:
                    continue
                yield timestamp, row
            else:
                # Flush the event streamer receiving data so that the lock on
                # the collection is released temporarily
                yield None