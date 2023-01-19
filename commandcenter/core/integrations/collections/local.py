from datetime import datetime, timedelta
from typing import AsyncIterable, Sequence

from commandcenter.core.integrations.abc import AbstractTimeseriesCollection
from commandcenter.core.integrations.models import BaseSubscription
from commandcenter.core.integrations.types import TimeseriesRow
from commandcenter.core.integrations.util.common import split_range
from commandcenter.core.timeseries.core import TimeseriesCollection



class LocalTimeseriesCollection(AbstractTimeseriesCollection):
    """Timeseries collection using the local backend API.
    
    All data is stored in memory and this class just implements the standard
    interface for a timeseries collection.

    Args:
        subscriptions: A sequence of the subscriptions to stream data for. The
            data will be streamed in sorted order of the subscriptions (using
            the hash of the subscription).
        delta: A timedelta for the stream period.
        collection: The backend timeseries collection to iterate from
    """
    def __init__(
        self,
        subscriptions: Sequence[BaseSubscription],
        delta: timedelta,
        collection: TimeseriesCollection
    ) -> None:
        super().__init__(subscriptions, delta)
        self._collection = collection
        self._closed = False

    async def close(self) -> None:
        self._closed = True

    async def __aiter__(self) -> AsyncIterable[TimeseriesRow]:
        if self._closed:
            raise RuntimeError("Collection is closed.")
        end = datetime.now()
        start = end - self._delta
        start_times, end_times = split_range(start, end, timedelta(minutes=15))
        for start_time, end_time in zip(start_times, end_times):
            view = self._collection.filter_by_subscription(self.subscriptions)
            async for timestamp, row in view.iter_range(start_time, end_time):
                yield timestamp, row