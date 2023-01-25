from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Set

from commandcenter.integrations.models import BaseSubscription
from commandcenter.timeseries.core.collection import TimeseriesCollection
from commandcenter.types import TimeseriesRow
from commandcenter.util import split_range



class LocalCollection(Iterable[TimeseriesRow]):
    """Streams from the core `TimeseriesCollection` data structure."""
    def __init__(
        self,
        subscriptions: Set[BaseSubscription],
        delta: timedelta | float,
        collection: TimeseriesCollection
    ) -> None:
        self._subscriptions = subscriptions
        if isinstance(delta, int):
            delta = timedelta(seconds=delta)
        if not isinstance(delta, timedelta):
            raise TypeError(f"Expected type 'timedelta | int', got {type(delta)}")
        self._delta = delta
        self._collection = collection

        self._closed = False
    
    def close(self) -> None:
        self._closed = True

    def __iter__(self) -> Iterable[TimeseriesRow]:
        if self._closed:
            raise RuntimeError("Collection is closed.")
        end = datetime.now()
        start = end - self._delta
        last_timestamp = None
        start_times, end_times = split_range(start, end, timedelta(minutes=15))
        for start_time, end_time in zip(start_times, end_times):
            if self._closed:
                break
            view = self._collection.filter_by_subscription(self._subscriptions)
            for i, (timestamp, row) in enumerate(view.iter_range(start_time, end_time)):
                if i < 2 and last_timestamp is not None and last_timestamp >= timestamp:
                    continue
                yield timestamp, row
            else:
                last_timestamp = timestamp
                # Signal the streamer to flush the buffer
                yield None
