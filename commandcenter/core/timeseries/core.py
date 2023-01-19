import asyncio
import bisect
import itertools
import logging
from collections import deque
from collections.abc import AsyncIterable, Iterable, MutableSequence, Sequence
from datetime import datetime, timedelta
from typing import (
    Any,
    AsyncGenerator,
    Deque,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast
)

from commandcenter.core.integrations.models import BaseSubscription
from commandcenter.core.integrations.types import TimeseriesRow
from commandcenter.core.timeseries.exceptions import ChunkLimitError, OldTimestampError


T = TypeVar("T")

_LOGGER = logging.getLogger("commandcenter.core.timeseries")


class Chunk(MutableSequence[T]):
    """A chunk is a mutable sequence-like object with a fixed length.
    
    Args:
        chunk_size: The max size of the chunk.
    """
    def __init__(self, chunk_size: int = 100) -> None:
        self._chunk_size = chunk_size
        self._data: List[T] = []

    @property
    def full(self) -> bool:
        """Returns `True` if length is greater than or equal to `chunk_size`."""
        return len(self) >= self._chunk_size

    def insert(self, index: int, value: T) -> None:
        if self.full:
            raise ChunkLimitError(self._chunk_size)
        self._data.insert(index, value)

    def append(self, value: T) -> None:
        """Append value to end of chunk.

        This method has O(1) time complexity.

        Returns:
            bool: `True` if value was successfully appended to chunk.
        """
        if self.full:
            raise ChunkLimitError(self._chunk_size)
        self._data.append(value)

    def pop(self, index: int) -> T:
        """Pop value from chunk.
        
        This method as O(n) time complexity depending on where the value is popped
        from. If it is the last index, the complexity is O(1) and O(n) otherwise.
        """
        return self._data.pop(index)

    def __getitem__(self, index: Union[int, slice]) -> Union[T, "Chunk[T]"]:
        if isinstance(index, slice):
            # slices return a new chunk instance
            start, stop, step = index.indices(len(self))
            values = itertools.islice(self._data, start, stop, step)
            chunk = self.__class__(self._chunk_size)
            for value in values:
                chunk.append(value)
            return chunk
        else:
            return self._data[index]

    def __setitem__(self, index: Union[int, slice], value: Union[T, Iterable[T]]) -> None:
        self._data[index] = value

    def __delitem__(self, index: Union[int, slice]) -> None:
        del self._data[index]

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Chunk):
            return False
        return self._data == __o._data

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, value: object) -> bool:
        return value in self._data

    def __iter__(self) -> Iterable[T]:
        for v in self._data:
            yield v

    
class TimeChunk(Chunk):
    """A time chunk is a modification of a chunk designed for datetime values.
    
    There are some key differences between `Chunk` and `TimeChunk`...
    - `TimeChunk` may only append `datetime.datetime` values
    - Values must be in monotonically increasing order otherwise the append
        operation will fail
    - Comparison operators (>, <, >=, <=) are implemented for a `TimeChunk`
    """
    def append(self, value: datetime) -> None:
        if not isinstance(value, datetime):
            raise TypeError(f"Expected 'datetime', got {type(value)}")
        if value <= self:
            # Only append monotically increasing values
            raise OldTimestampError(value)
        return super().append(value)

    def __gt__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, TimeChunk)):
            raise TypeError(f"'>' not supported between instances of {type(self)} and {type(__o)}")
        try:
            if isinstance(__o, TimeChunk):
                return self[-1] > __o[0] 
            else:
                return self[-1] > __o
        except IndexError:
            return False

    def __ge__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, TimeChunk)):
            raise TypeError(f"'>=' not supported between instances of {type(self)} and {type(__o)}")
        try:
            if isinstance(__o, TimeChunk):
                return self[-1] >= __o[0] 
            else:
                return self[-1] >= __o
        except IndexError:
            return False

    def __lt__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, TimeChunk)):
            raise TypeError(f"'<' not supported between instances of {type(self)} and {type(__o)}")
        try:
            if isinstance(__o, TimeChunk):
                return self[0] < __o[-1] 
            else:
                return self[0] < __o
        except IndexError:
            return False

    def __le__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, TimeChunk)):
            raise TypeError(f"'<=' not supported between instances of {type(self)} and {type(__o)}")
        try:
            if isinstance(__o, TimeChunk):
                return self[0] < __o[-1] 
            else:
                return self[0] < __o
        except IndexError:
            return False
        

def validate_sample(sample: Tuple[datetime, Any]) -> None:
    if not isinstance(sample, tuple):
        raise TypeError(f"Expected type 'tuple', got {type(sample)}")
    if len(sample) != 2:
        raise ValueError(f"Expect tuple of length 2, got {len(sample)}")


class Timeseries:
    """A timeseries is an append only sequence-like timeseries data structure.
    
    Internally, `Timeseries` is made up of doubly linked lists of fixed length
    memory chunks representing a sequence of sample tuples (timestamp, value).
    A `Timeseries` is uniquely identified by its subscription which inherits
    from `BaseSubscription`.

    A timeseries is always monotonically increasing, you cannot add a sample
    where the timestamp of the sample is less than the maximum timestamp in the
    series.
    
    Outdated chunks are evicted from the series according to the retention
    period. The minimum retained timestamp is calculated against the maximum
    timestamp in the series. If a chunks last timestamp is less than the minumum
    retained timestamp, that chunk is evicted from the series.

    Timeseries are not thread safe.

    Args:
        subscription: The id of the timeseries
        samples: Any initial samples that should be added to the timeseries. These
            must be sorted by timestamp in monotonically increasing order otherwise
            and `OldTimestampError` will be raised
        retention: The retention period of the series (in milliseconds)
        chunk_size: The fixed length of chunks in the timeseries
        labels: Meta data for querying timeseries in a collection
    """
    def __init__(
        self,
        subscription: BaseSubscription,
        samples: Iterable[Tuple[datetime, Any]] = None,
        retention: int = 7200000,
        chunk_size: int = 100,
        labels: Sequence[str] = None
    ) -> None:
        self.subscription = subscription
        self._retention = retention
        self._chunk_size = chunk_size

        self.labels: Set[str] = set(labels) if labels else set()
        self._min_range: datetime = None
        self._max_range: datetime = None
        
        self._timechunks: Deque[TimeChunk] = deque()
        self._valchunks: Deque[Chunk] = deque()

        self._lock: asyncio.Lock = asyncio.Lock()

        if samples is not None:
            self.madd(samples)

    @property
    def min_retained(self) -> datetime:
        """The minumum retained timestamp in the series."""
        if self._timechunks:
            if self._retention:
                max_existing = self._timechunks[-1][-1]
                return max_existing - timedelta(seconds=self._retention/1000)
            return min(self._timechunks[0])

    async def add(self, sample: Tuple[datetime, T]) -> None:
        """Add a sample to the time series.
        
        The retention policy is applied after the operation.

        Args:
            sample: Tuple of (timestamp, value) pairs
        
        Raises:
            OldTimestampError: If the sample time is less than the most recent
                value in the time series
        """
        await self._add(sample)
        await self._apply_retention()

    async def madd(self, samples: Iterable[Tuple[datetime, Any]]) -> None:
        """Add multiple samples to the time series.
        
        The retention policy is applied after all samples have been added.
        Samples must be sorted chronologically otherwise this operation will raise
        an `OldTimestampError`.

        Args:
            sample: Iterable of tuples of (timestamp, value) pairs
        
        Raises:
            OldTimestampError: If the sample time is less than the most recent
                value in the time series
        """
        try:
            for sample in samples:
                await self._add(sample)
        finally:
            await self._apply_retention()

    async def iter_chunks(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None
    ) -> AsyncGenerator[Tuple[TimeChunk, Chunk], bool]:
        """Return a subset of chunks from the full timeseries range.
        
        The chunks are copies and can be safely mutated without affecting the
        underlying timeseries.

        If `start` is None, iterate from beginning. If `end` is None, iterate to
        end of series.

        Args:
            start: The start range to iterate from
            end: The end range to iterate to

        Yields:
            chunks: Tuple of (TimeChunk, Chunk)

        Raises:
            ValueError: Raised if `end` <= `start`
        """
        async with self._lock:
            start_index = 0
            end_index = len(self._timechunks)
            if start is not None and end is not None and end <= start:
                raise ValueError("Invalid range. 'end' cannot be less than 'start'")
            if start is not None:
                start_index = max(bisect.bisect_left(self._timechunks, start) - 1, 0)
            if end is not None:
                end_index = min(bisect.bisect_left(self._timechunks, end) + 1, len(self._timechunks))

            min_retained = self.min_retained

            for i, (timechunk, valchunk) in enumerate(
                zip(
                    itertools.islice(self._timechunks, start_index, end_index),
                    itertools.islice(self._valchunks, start_index, end_index)
                )
            ):
                if i == 0: # First chunk 
                    index = bisect.bisect_left(timechunk, min_retained)
                    if start is not None:
                        index = max(bisect.bisect_left(timechunk, start)-1, index)
                    timechunk, valchunk = timechunk[index:], valchunk[index:]
                    while True:
                        # The chunk receiver has the option to slice the chunk and
                        # send the remain back to the generator where it will be
                        # served up on the next iteration
                        remain = yield timechunk, valchunk
                        if not remain:
                            break
                        else:
                            timechunk, valchunk = remain
                            yield
                else:
                    timechunk, valchunk = timechunk[:], valchunk[:]
                    while True:
                        remain = yield timechunk, valchunk
                        if not remain:
                            break
                        else:
                            timechunk, valchunk = remain
                            yield

    async def iter_range(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None
    ) -> AsyncIterable[Tuple[datetime, Any]]:
        """Return a subset of the full timeseries range.
        
        The range is always an outer range meaning the first sample will be
        the last timestamp less than `start` and the last sample will be the first
        timestamp greater than `end`

        If `start` is None, iterate from beginning. If `end` is None, iterate to
        end of series.

        Args:
            start: The start range to iterate from
            end: The end range to iterate to

        Yields:
            sample: Tuple of (timestamp, value)

        Raises:
            ValueError: Raised if `end` <= `start`
        """
        async with self._lock:
            start_index = 0
            end_index = len(self._timechunks)
            if start is not None and end is not None and end <= start:
                raise ValueError("Invalid range. 'end' cannot be less than 'start'")
            if start is not None:
                start_index = max(bisect.bisect_left(self._timechunks, start) - 1, 0)
            if end is not None:
                end_index = min(bisect.bisect_left(self._timechunks, end) + 1, len(self._timechunks))
            
            min_retained = self.min_retained
            
            last_t: datetime = None
            last_v: Any = None

            for timestamp, value in zip(
                itertools.chain.from_iterable(
                    itertools.islice(self._timechunks, start_index, end_index)
                ),
                itertools.chain.from_iterable(
                    itertools.islice(self._valchunks, start_index, end_index)
                )
            ):
                try:
                    # Dont yield any samples less than the retention period
                    if timestamp < min_retained:
                        continue
                    # Dont yield any samples where the timestamp is outside the start range
                    elif start is not None and timestamp < start:
                        continue
                    # The range is an outer boundary so we yield the last sample
                    # (outside the range) and the current sample
                    elif start is not None and last_t < start and timestamp > start:
                        for v in ((last_t, last_v), (timestamp, value)):
                            yield v
                    # The last value yielded is the first timestamp outside the end
                    # range
                    elif end is not None and last_t < end and timestamp > end:
                        yield timestamp, value
                        break
                    # Else yield. If start and end are none, every value inside the
                    # retention period is yielded
                    else:
                        yield timestamp, value
                finally:
                    last_t, last_v = timestamp, value

    async def _add(self, sample: Tuple[datetime, Any]) -> None:
        """Validate and add sample to chunks. This creates new chunks if required."""
        validate_sample(sample)
        async with self._lock:
            try:
                self._timechunks[-1].append(sample[0])
                self._valchunks[-1].append(sample[1])
            except (IndexError, ChunkLimitError):
                for chunk, v, chunks in zip(
                    (TimeChunk(self._chunk_size), Chunk(self._chunk_size)),
                    sample,
                    (self._timechunks, self._valchunks)
                ):
                    chunk.append(v)
                    chunks.append(chunk)

    async def _apply_retention(self) -> None:
        """Pop chunks if the time chunk is less than the minimum retained timestamp."""
        async with self._lock:
            min_retained = self.min_retained
            if min_retained is not None:
                # `self._timechunks[0] > min_retained = True` means at least one timestamp
                # in the chunk should be retained which means we retain the whole chunk 
                while len(self._timechunks) > 1 and not self._timechunks[0] > min_retained:
                    chunk = self._timechunks.popleft()
                    self._valchunks.popleft()
                    _LOGGER.debug("Evicting chunk up to %s", chunk[-1])

    def __getitem__(self, index: Union[int, slice]) -> Tuple[datetime, Any]:
        if isinstance(index, slice):
            raise TypeError("timeseries index must be integer, not 'slice'")
        
        l = len(self)
        if not self._timechunks or index >= l:
            raise IndexError("timeseries index out of range")
        if index < 0:
            index = l + index
        
        # This gets us the index of the target chunk
        chunk = index//self._chunk_size
        # This gets us the index within the chunk
        chunk_index = index%self._chunk_size

        return self._timechunks[chunk][chunk_index], self._valchunks[chunk][chunk_index]
        
    def __len__(self) -> int:
        if not self._timechunks:
            return 0
        min_retained = self.min_retained
        start_index = bisect.bisect_left(self._timechunks[0], min_retained)
        # Dont count the samples outside the retention period
        l = len(self._timechunks[0]) - start_index
        if len(self._timechunks) == 1:
            return l
        else:
            # The "middle" chunks are a fixed size so we just
            # need to add the length of the first chunk, the last chunk, and
            # the number of middle chunks * `_chunk_size`
            return l + (len(self._timechunks) - 2)*self._chunk_size + len(self._timechunks[-1])

    async def __aiter__(self) -> AsyncIterable[Tuple[datetime, Any]]:
        async for sample in self.iter_range():
            yield sample

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Timeseries):
            return False
        return self.subscription == __o.subscription

    def __gt__(self, __o: object) -> bool:
        if not isinstance(__o, Timeseries):
            raise TypeError(f"'>' not supported between instances of {type(self)} and {type(__o)}")
        try:
            return self.subscription > __o.subscription
        except TypeError:
            return False

    def __lt__(self, __o: object) -> bool:
        if not isinstance(__o, Timeseries):
            raise TypeError(f"'<' not supported between instances of {type(self)} and {type(__o)}")
        try:
            return self.subscription < __o.subscription
        except TypeError:
            return False


class TimeseriesCollection:
    """A timeseries collection is an indexed container of timeseries objects.
    
    Timeseries are indexed by their subscription and can be accessed via the
    index operator ([]) similar to a mapping.

    You should normally only ever need 1 `TimeseriesCollection` for the runtime
    of an application and it should be the primary point for creating and iterating
    `Timeseries` objects 
    """
    def __init__(self) -> None:
        self._series: Dict[int, "Timeseries"] = {}
        self._lock: asyncio.Lock = asyncio.Lock()

    def __getitem__(self, index: BaseSubscription) -> "Timeseries":
        return self._series[hash(index)]

    async def create(
        self,
        subscription: BaseSubscription,
        samples: Iterable[Tuple[datetime, Any]] = None,
        retention: int = 7200,
        chunk_size: int = 100,
        labels: Sequence[str] = None,
        overwrite: bool = False
    ) -> "Timeseries":
        """Create a `Timeseries`.
        
        If the subscription already exists, this will return the existing timeseries
        object unless `overwrite=True` in which case the new object will replace
        the old.

        Args:
            subscription: The id of the timeseries
            samples: Any initial samples that should be added to the timeseries. These
                must be sorted by timestamp in monotonically increasing order otherwise
                and `OldTimestampError` will be raised
            retention: The retention period of the series (in milliseconds)
            chunk_size: The fixed length of chunks in the timeseries
            labels: Meta data for querying timeseries in a collection
            overwrite: If `True` overwrite the existing `Timeseries` if it already exists

        Returns:
            timeseries

        Raises:
            OldTimestampError: If the samples are provided but the timestamps
                are not sorted
        """
        async with self._lock:
            h = hash(subscription)
            if h in self._series and not overwrite:
                return self._series[h]
            series = Timeseries(subscription, samples, retention, chunk_size, labels)
            self._series[h] = series
            _LOGGER.debug("Created new timeseries: %s", subscription.json())
            return series

    def filter_by_subscription(
        self,
        subscriptions: Sequence[BaseSubscription]
    ) -> "TimeseriesCollectionView":
        """Query a subset of the collection by label and return a `TimeseriesCollectionView`.

        Args:
            subscriptions: A sequence of subscriptions to query
        
        Returns:
            view: TimeseriesCollectionView
        """
        subscriptions: Set[BaseSubscription] = set(subscriptions)

        async def _filter() -> Iterable["Timeseries"]:
            async with self._lock:
                for series in self._series.values():
                    if series.subscription in subscriptions:
                        yield series
        
        series = _filter()
        return TimeseriesCollectionView(series)

    def filter_by_labels(
        self,
        labels: Sequence[str] = None,
    ) -> "TimeseriesCollectionView":
        """Query a subset of the collection by label and return a `TimeseriesCollectionView`.

        Args:
            labels: A sequence of labels to query
        
        Returns:
            view: TimeseriesCollectionView
        """
        labels: Set[str] = set(labels)

        async def _filter() -> Iterable["Timeseries"]:
            with self._lock:
                for series in self._series.values():
                    if labels.difference(series.labels) != labels:
                        yield series

        series = _filter()
        return TimeseriesCollectionView(series)

    async def __aiter__(self) -> AsyncIterable[TimeseriesRow]:
        subscriptions = [series.subscription for series in self._series.values()]
        view = self.filter_by_subscription(subscriptions)
        async for row in view:
            yield row


class TimeseriesCollectionView(AsyncIterable[TimeseriesRow]):
    """A collection view that is a one time use iterable for viewing a subset
    of a collection of timeseries.
    
    Views should never be created directly, they should only ever be created by
    a `TimeseriesCollection`

    `TimeseriesCollectionViews` are highly memory efficient, they do not copy
    any data from the parent collection and iterating over them works chunk by
    chunk for each timeseries.
    """
    def __init__(self, series: AsyncIterable["Timeseries"]) -> None:
        self._series = series

    def filter_by_subscription(
        self,
        subscriptions: Sequence[BaseSubscription]
    ) -> "TimeseriesCollectionView":
        """Query a subset of the collection by label and return a `TimeseriesCollectionView`.

        Args:
            subscriptions: A sequence of subscriptions to query
        
        Returns:
            view: TimeseriesCollectionView
        """
        subscriptions: Set[BaseSubscription] = set(subscriptions)

        async def _filter() -> Iterable["Timeseries"]:
            async for series in self._series:
                if series.subscription in subscriptions:
                    yield series
        
        series = _filter()
        return TimeseriesCollectionView(series)

    def filter_by_labels(
        self,
        labels: Sequence[str] = None,
    ) -> "TimeseriesCollectionView":
        """Query a subset of the collection by label and return a `TimeseriesCollectionView`.

        Args:
            labels: A sequence of labels to query
        
        Returns:
            view: TimeseriesCollectionView
        """
        labels: Set[str] = set(labels)

        async def _filter() -> Iterable["Timeseries"]:
            async for series in self._series:
                if labels.difference(series.labels) != labels:
                    yield series

        series = _filter()
        return TimeseriesCollectionView(series)

    async def iter_range(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None
    ) -> AsyncIterable[Tuple[datetime, List[Any]]]:
        """Return a subset of the collection view.
        
        This returns a full timeseries collection row by row where a row is a list
        where the first index is always the timestamp. Every subsequent index is
        a value in the timeseries. If a timeseries does not have a value at that
        timestamp, the value will be None.

        The first row is always the index row which identifies the timeseries for
        values in subsequent rows. The id of a timeseries is the hash of the timeseries
        subscription. The hash's are stable so a consistent order is always
        maintained.

        Args:
            start: The start range to iterate from
            end: The end range to iterate to

        Yields:
            row: List of [tn, v1, ... vn] where tn is a unique timestamp and vn
                is a JSONPrimitive

        Raises:
            ValueError: Raised if `end` <= `start`
        """
        index, iters = [], []
        async for series in self._series:
            index.append(hash(series.subscription))
            iters.append(series.iter_chunks(start, end))
        
        if not iters:
            return
        
        index, iters = zip(*sorted(zip(index, iters), key=lambda x: x[0]))

        iters = cast(List[AsyncGenerator[Tuple[TimeChunk, Chunk], bool]], iters)

        yield index
        # Loop until we have exhausted all chunk iterators for each timeseries
        while True:
            timestamps: Set[datetime] = set()
            chunks: List[Tuple[TimeChunk, Chunk]] = []
            stop = 0
            for iter_ in iters:
                try:
                    chunk_tup = await iter_.__anext__()
                    chunks.append(chunk_tup)
                except StopAsyncIteration:
                    # Exhausted chunk iterator for this series
                    chunks.append(([], []))
                    stop += 1
            if stop == len(iters):
                # All chunk iterators exhausted, we're done
                break

            min_t = None
            for timechunk, _ in chunks:
                if timechunk:
                    min_t = timechunk[-1] if min_t is None or timechunk[-1] < min_t else min_t
                    # We want each timeseries to have a value for each timestamp so we
                    # create a sorted index of all timestamps in each chunk
                    timestamps.update(timechunk)

            for i in range(len(chunks)):
                timechunk = chunks[i][0]
                valchunk = chunks[i][1]
                # We can only iterate up to the minumum timestamp in the collection
                # of timeseries (min([chunk[0][-1] for chunk in chunks]))
                # If we go to max([chunk[0][-1] for chunk in chunks]) then next
                # chunk for the series satisfying min([chunk[0][-1] for chunk in chunks])
                # may yield a duplicate or out of order timestamp.
                if timechunk and timechunk[-1] > min_t:
                    index = bisect.bisect_left(timechunk, min_t)
                    if timechunk[index] == min_t:
                        index += 1
                    # Slice the time and value chunks and send them back to the
                    # iterators
                    if index == len(timechunk):
                        continue
                    elif index == 0:
                        await iters[i].asend((timechunk, valchunk))
                        chunks[i] = ([], [])
                    else:
                        await iters[i].asend((timechunk[index:], valchunk[index:]))
                        chunks[i] = (timechunk[:index], valchunk[:index])

            for timestamp in sorted(timestamps):
                if timestamp > min_t:
                    break
                row = []
                # For each timestamp in the index we check the timestamp of the
                # current chunk and if they line up we add the value of the chunk
                # to the row otherwise None
                for timechunk, valchunk in chunks:
                    try:
                        if timechunk[0] == timestamp:
                            # The chunks are copies so we can pop without affecting
                            # the underlying dataset
                            row.append(valchunk.pop(0))
                            timechunk.pop(0)
                        else:
                            row.append(None)
                    except IndexError:
                        row.append(None)
                yield timestamp, row

    async def __aiter__(self) -> AsyncIterable[Tuple[datetime, List[Any]]]:
        async for timestamp, row in self.iter_range():
            yield timestamp, row