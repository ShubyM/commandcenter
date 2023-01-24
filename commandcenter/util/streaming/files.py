import csv
import functools
import io
import logging
from collections.abc import AsyncIterable, AsyncIterator
from typing import Any, Callable, List

import jsonlines

from commandcenter.types import JSONPrimitive, TimeseriesRow



_LOGGER = logging.getLogger("commandcenter.util.streaming.files")


def jsonlines_writer(buffer: io.StringIO) -> jsonlines.Writer:
    """A writer, buffer combo for JSONlines streaming."""
    writer = jsonlines.Writer(buffer)
    return functools.partial(writer.write)


def csv_writer(buffer: io.StringIO) -> csv.writer:
    """A writer, buffer combo for CSV streaming."""
    writer = csv.writer(buffer, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    return functools.partial(writer.writerow)


def timeseries_row_formatter(row: TimeseriesRow) -> List[JSONPrimitive]:
    """Format timeseries row to `List[JSONPrimitive]`."""
    return [row[0].isoformat(), *row[1]]


async def chunk_generator(
    iterator: AsyncIterator[Any],
    buffer: io.StringIO,
    writer: Callable[[Any], None],
    formatter: Callable[[Any], Any],
    chunk_size: int = 1000
) -> AsyncIterable[str]:
    """Stream rows of data in chunks.
    
    Each row is appended to the buffer up to `chunk_size` at which point a
    flush is triggered and the buffer is cleared.

    Empty rows or anything evaluating `if not row` to `True` will trigger
    a flush as well.

    Args:
        iterator: The object to iterate over.
        buffer: The buffer that will hold data. Must be a an IOBase like object.
        formatter: A callable that accepts the raw output from the iterator and
            formats it so it can be understood by the writer.
        witer: A callable that accepts data from the formatter and writes the
            data to the buffer.
        chunk_size: The max number of iterations before a flush is triggered.

    Raises:
        Exception: Any exception raise by the iterator.
    """
    count = 0
    while True:
        try:
            row = await iterator.__anext__()
        except StopAsyncIteration:
            chunk = buffer.getvalue()
            if chunk:
                yield chunk
            return
        except Exception:
            _LOGGER.warning("Unhandled exception in iterator", exc_info=True)
            raise
        
        # Empty rows are breaks, yield the chunk
        if not row:
            chunk = buffer.getvalue()
            if chunk:
                yield chunk
            chunk_size = 0
            continue
        
        try:
            writer(formatter(row))
        except Exception:
            _LOGGER.warning("Unhandled error in writer", exc_info=True)
            raise

        count += 1
        if count >= chunk_size:
            chunk = buffer.getvalue()
            if chunk:
                yield chunk
            buffer.seek(0)
            buffer.truncate(0)
            count = 0