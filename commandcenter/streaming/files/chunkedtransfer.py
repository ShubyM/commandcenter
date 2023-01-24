import inspect
import io
import logging
from collections.abc import AsyncIterable
from typing import Any, Callable



_LOGGER = logging.getLogger("commandcenter.streaming.files")


async def file_chunked_transfer(
    iterable: AsyncIterable[Any],
    buffer: io.BytesIO | io.StringIO,
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
        buffer: The buffer that will hold data.
        formatter: A callable that accepts the raw output from the iterator and
            formats it so it can be understood by the writer.
        witer: A callable that accepts data from the formatter and writes the
            data to the buffer.
        chunk_size: The max number of iterations before a flush is triggered.

    Raises:
        Exception: Any exception raise by the iterator.
    """
    count = 0
    try:
        while True:
            try:
                data = await iterable.__anext__()
            except StopAsyncIteration:
                chunk = buffer.getvalue()
                if chunk:
                    yield chunk
                return
            except Exception:
                _LOGGER.warning("Unhandled exception in iterator", exc_info=True)
                if inspect.isasyncgen(iterable):
                    try:
                        await iterable.aclose()
                    except:
                        pass
                raise
            
            # Empty rows are breaks, yield the chunk
            if not data:
                chunk = buffer.getvalue()
                if chunk:
                    yield chunk
                chunk_size = 0
                continue
            
            try:
                writer(formatter(data))
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
    
    finally:
        if inspect.isasyncgen(iterable):
            try:
                await iterable.aclose()
            except:
                pass
        