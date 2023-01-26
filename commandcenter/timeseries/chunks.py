import itertools
from collections.abc import Iterable, MutableSequence
from datetime import datetime
from typing import List, TypeVar, Union

from commandcenter.timeseries.core.exceptions import ChunkLimitError, OldTimestampError



T = TypeVar("T")


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