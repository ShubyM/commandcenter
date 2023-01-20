from datetime import datetime, timedelta
from typing import AsyncIterable, Sequence

from commandcenter.core.integrations.abc import AbstractTimeseriesCollection
from commandcenter.core.integrations.models import BaseSubscription
from commandcenter.core.integrations.types import TimeseriesRow
from commandcenter.core.integrations.util.common import split_range
from commandcenter.core.timeseries.collection import TimeseriesCollection



class LocalTimeseriesCollection(AbstractTimeseriesCollection):
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
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def __aiter__(self) -> AsyncIterable[TimeseriesRow]:
        if self.closed:
            raise RuntimeError("Collection is closed.")
        end = datetime.now()
        start = end - self.delta
        # When iterating over a collection, those timeseries are locked.
        # We assume, data for a timeseries collection is being streamed over
        # a network connection so we break the range into sub-ranges so we can
        # chunk the data and send it. While being sent, data waiting to be added
        # can be added.
        start_times, end_times = split_range(start, end, timedelta(minutes=15))
        for start_time, end_time in zip(start_times, end_times):
            view = self.collection.filter_by_subscription(self.subscriptions)
            async for timestamp, row in view.iter_range(start_time, end_time, boundary="chunked"):
                yield timestamp, row