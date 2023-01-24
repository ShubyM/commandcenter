import asyncio
import bisect
import itertools
import logging
from collections import deque
from collections.abc import AsyncIterable, Iterable, Sequence
from datetime import datetime, timedelta
from typing import (
    Any,
    AsyncGenerator,
    Deque,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from commandcenter.integrations.models import BaseSubscription
from commandcenter.timeseries.chunks import Chunk, TimeChunk
from commandcenter.timeseries.exceptions import ChunkLimitError



T = TypeVar("T")

_LOGGER = logging.getLogger("commandcenter.core.timeseries")


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