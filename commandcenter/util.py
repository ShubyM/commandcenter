import csv
import functools
import io
import json
import logging
import pathlib
import random
import re
import subprocess
from collections.abc import AsyncIterable, Awaitable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum, IntEnum
from typing import Any, Callable, Dict, List, Tuple, Type

import anyio
import dateutil.parser
import jsonlines
import ndjson
import orjson
import pendulum
from accept_types import get_best_match
from pendulum.datetime import DateTime
from ratelimit.auths import EmptyInformation
from starlette.authentication import BaseUser
from starlette.types import Scope
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from commandcenter.exceptions import NonZeroExitCode
from commandcenter.types import JSONPrimitive, TimeseriesRow


"""A collection of objects and functions used throughout various modules in
commandcenter.

If the number of util functions grows unwieldy we might eventually break it up
but for now we enjoy the mess.
"""


TIMEZONE = pendulum.now().timezone_name

capitalize_pattern = re.compile('(?<!^)(?=[A-Z])')


class ObjSelection(str, Enum):
    """Define a selection of objects.
    
    Examples:
    >>> class AuthBackends(ObjSelection):
    ...     DEFAULT = "default", ActiveDirectoryBackend
    ...     ACTIVE_DIRECTORY = "activedirectory", ActiveDirectoryBackend

    Now we can select a backend by a key
    >>> backend = AuthBackends("activedirectory").cls
    
    This is particularly useful in confguration
    >>> config = Config(".env")
    >>> BACKEND = config(
    ...     "BACKEND",
    ...     cast=lambda v: AuthBackends(v).cls,
    ...     default=AuthBackends.default.value
    ... )
    """
    def __new__(cls, value: str, type_: Type[Any]) -> "ObjSelection":
        obj = str.__new__(cls, value)
        obj._value_ = value
        
        obj.cls = type_  # type: ignore[attr-defined]
        return obj


def snake_to_camel(string: str) -> str:
    """Covert snake case (arg_a) to camel case (ArgA)."""
    return ''.join(word.capitalize() for word in string.split('_'))


def snake_to_lower_camel(string: str) -> str:
    """Covert snake case (arg_a) to lower camel case (argA)."""
    splits = string.split('_')
    if len(splits) == 1:
        return string
    return f"{splits[0]}{''.join(word.capitalize() for word in splits[1:])}"


def camel_to_snake(string: str) -> str:
    """Convert camel (ArgA) and lower camel (argB) to snake case (arg_a)"""
    return capitalize_pattern.sub('_', string).lower()


def run_subprocess(
    command: List[str],
    logger: logging.Logger,
    raise_non_zero: bool = True
) -> None:
    """Run a subprocess and route the `stdout` and `stderr` to the logger.
    
    Stdout is debug information and stderr is warning.

    Raises:
        NonZeroExitCode: Process exited with non-zero exit code.
    """
    with subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as process:
        for line in process.stdout.readlines():
            logger.debug(line.decode().rstrip("\r\n"))
        for line in process.stderr.readlines():
            logger.warning(line.decode().rstrip("\r\n"))
    if process.returncode != 0:
        logger.warning("Process exited with non-zero exit code (%i)", process.returncode)
        if raise_non_zero:
            raise NonZeroExitCode(process.returncode)


def json_loads(v: str | bytes):
    """JSON decoder which uses orjson for bytes and builtin json for str."""
    match v:
        case str():
            return json.loads(v)
        case bytes():
            return orjson.loads(v)
        case _:
            raise TypeError(f"Expected str | bytes, got {type(v)}")


def isoparse(timestamp: str) -> DateTime:
    """Parse iso8601 string to datetime."""
    return pendulum.instance(dateutil.parser.isoparse(timestamp))


def in_timezone(timestamp: str | datetime | DateTime, timezone: str) -> DateTime:
    """Parse iso8601 timestamp or DateTime object to DateTime in specified timezone."""
    match timestamp:
        case str():
            return isoparse(timestamp).in_timezone(timezone).replace(tzinfo=None)
        case datetime():
            return pendulum.instance(timestamp).in_timezone(timezone).replace(tzinfo=None)
        case DateTime():
            return timestamp.in_timezone(timezone).replace(tzinfo=None)
        case _:
            raise TypeError(f"Expected str | datetime | DateTime, got {type(timestamp)}")


def split_range(
    start_time: datetime,
    end_time: datetime,
    dt: timedelta
) -> Tuple[List[datetime], List[datetime]]:
    """Split a time range into smaller ranges."""
    start_times = []
    end_times = []
    
    while start_time < end_time:
        start_times.append(start_time)
        next_timestamp = start_time + dt
        
        if next_timestamp >= end_time:
            start_time = end_time
        
        else:
            start_time = next_timestamp
        end_times.append(start_time)
    
    return start_times, end_times


def get_timestamp_index(data: List[Dict[str, Any]]) -> List[str]:
    """Create a single, sorted timestamp index from a chunk of timeseries
    data potentially containing duplicate timestamps.
    
    Duplicate timestamps are removed.
    """
    index = set()
    for datum in data:
        index.update(datum["timestamp"])
    return sorted(index)


def iter_timeseries_rows(
    index: List[str],
    data: List[Dict[str, List[Any]]],
    timezone: str | None = None
) -> Iterable[TimeseriesRow]:
    """Iterate a collection of timeseries data row by row and produce rows
    which have data aligned on a common timestamp.

    Note: The data must be in monotonically increasing order for this to work
    correctly.
    """
    for timestamp in index:
        row = []
        for datum in data:
            try:
                if datum["timestamp"][0] == timestamp:
                    row.append(datum["value"].pop(0))
                    datum["timestamp"].pop(0)
                else:
                    # Most recent data point is later than current timestamp
                    row.append(None)
            except IndexError:
                # No more data for that web id
                row.append(None)
        if timezone is not None:
            timestamp = in_timezone(timestamp, timezone)
        if isinstance(timestamp, str):
            timestamp = isoparse(timestamp)
        yield timestamp, row


def format_timeseries_rows(row: TimeseriesRow) -> List[JSONPrimitive]:
    """Formats a timeseries row as an iterable which can be converted to a row
    for a file format.
    """
    return [row[0].isoformat(), *row[1]]

class EqualJitterBackoff:
    """Equal jitter backoff upon failure"""

    def __init__(self, cap: float, initial: float):
        self.cap=cap
        self.initial = initial

    def compute(self, failures):
        temp = min(self.cap, self.initial * 2**failures) / 2
        return temp + random.uniform(0, temp)


def ndjson_writer(buffer: io.StringIO) -> Callable[[Any], None]:
    """A writer for ndjson streaming."""
    writer = ndjson.writer(buffer, ensure_ascii=False)
    return functools.partial(writer.writerow)


def jsonlines_writer(buffer: io.StringIO) -> Callable[[Any], None]:
    """A writer for JSONlines streaming."""
    writer = jsonlines.Writer(buffer)
    return functools.partial(writer.write)


def csv_writer(buffer: io.StringIO) -> Callable[[Iterable], None]:
    """A writer for CSV streaming."""
    writer = csv.writer(buffer, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    return functools.partial(writer.writerow)


def get_file_format_writer(accept: str) -> "FileWriter":
    """Matches the accept header to a file format writer."""
    accept_types = [
        "text/csv",
        "application/jsonlines",
        "application/x-jsonlines",
        "application/x-ndjson"
    ]
    best_match = get_best_match(accept.lower(), accept_types)
    buffer = io.StringIO()
    
    match best_match:
        case "text/csv":
            writer = csv_writer(buffer)
            return FileWriter(buffer, writer, ".csv", "text/csv")
        case "application/jsonlines" | "application/x-jsonlines":
            writer = jsonlines_writer(buffer)
            return FileWriter(buffer, writer, ".jsonl", "application/x-jsonlines")
        case "application/x-ndjson":
            writer = ndjson_writer(buffer)
            return FileWriter(buffer, writer, ".ndjson", "application/x-ndjson")
        case _:
            raise ValueError()


@dataclass
class FileWriter:
    buffer: io.StringIO
    writer: Callable[[Any], None]
    suffix: str
    media_type: str


async def chunked_transfer(
    send: AsyncIterable[Any],
    buffer: io.BytesIO | io.StringIO,
    writer: Callable[[Any], None],
    formatter: Callable[[Any], Any],
    logger: logging.Logger,
    chunk_size: int = 1000
) -> AsyncIterable[str]:
    """Stream rows of data in chunks.
    
    Each row is appended to the buffer up to `chunk_size` at which point a
    flush is triggered and the buffer is cleared.

    `None` is considered a break and will trigger a flush.

    Args:
        iterator: The object to iterate over.
        buffer: The buffer that will hold data.
        formatter: A callable that accepts the raw output from the iterator and
            formats it so it can be understood by the writer.
        witer: A callable that accepts data from the formatter and writes the
            data to the buffer.
        chunk_size: The max number of iterations before a flush is triggered.

    Raises:
        Exception: Any exception raise by the iterator, writer, or formatter.
    """
    count = 0
    try:
        async for data in send:
            if data is None:
                chunk = buffer.getvalue()
                if chunk:
                    yield chunk
                buffer.seek(0)
                buffer.truncate(0)
                chunk_size = 0
                continue
            
            try:
                writer(formatter(data))
            except Exception:
                logger.error("Unhandled error in writer", exc_info=True)
                raise
            
            count += 1
            if count >= chunk_size:
                chunk = buffer.getvalue()
                if chunk:
                    yield chunk
                buffer.seek(0)
                buffer.truncate(0)
                count = 0
        else:
            chunk = buffer.getvalue()
            if chunk:
                yield chunk
            return
    except Exception:
        logger.error("Unhandled exception in send", exc_info=True)


async def sse_handler(send: AsyncIterable[Any], logger: logging.Logger) -> AsyncIterable[Any]:
    """Wraps an async iterable, yields events."""
    try:
        async for msg in send:
            yield msg
    except Exception:
        logger.error("Connection closed abnormally", exc_info=True)


async def ws_handler(
    websocket: WebSocket,
    logger: logging.Logger,
    receive: Callable[[WebSocket], Awaitable[None]] | None = None,
    send: AsyncIterable[Any] | None = None
) -> None:
    """Manages a websocket connection."""
    async def wrap_send() -> None:
        async for data in send:
            match data:
                case str():
                    await websocket.send_text(data)
                case bytes():
                    await websocket.send_bytes(data)
                case _:
                    await websocket.send_json(data)

    receive = receive or _null_receive

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(receive(websocket))
            tg.start_soon(wrap_send(send))

    except WebSocketDisconnect as e:
        logger.debug(
            "Websocket connection closed by user",
            extra={
                "code": e.code,
                "reason": e.reason
            }
        )
    
    except Exception:
        logger.error("Connection closed abnormally", exc_info=True)
        try:
            await websocket.close(1006)
        except Exception:
            pass

    finally:
        if websocket.state != WebSocketState.DISCONNECTED:
            try:
                await websocket.close(1006)
            except:
                pass


async def _null_receive(websocket: WebSocket) -> None:
    """Receives messages from a websocket but does nothing with them."""
    while True:
        msg = await websocket.receive()
        if msg["type"] == "websocket.disconnect":
            code = msg["code"]
            if isinstance(code, IntEnum): # wsproto
                raise WebSocketDisconnect(code=code.value, reason=code.name)
            # websockets
            raise WebSocketDisconnect(code=code.code, reason=code.reason)


def cast_path(path: str | None) -> pathlib.Path:
    """Cast a non-empty string to a path."""
    if path:
        return pathlib.Path(path)
    return path


def cast_logging_level(level: str | int) -> int:
    """Cast a logging level as str or int to int."""
    try:
        return int(level)
    except ValueError:
        match level.lower():
            case "notset":
                return 0
            case "debug":
                return 10
            case "info":
                return 20
            case "warning":
                return 30
            case "error":
                return 40
            case "critical":
                return 50
            case _:
                return 0


async def username_limit(scope: Scope) -> Tuple[str, str]:
    """Auth backend for rate limiting based on username.
    
    This requires the 'user' key in the scope, therefore the `AuthenticationMiddleware`
    must be installed in the stack before the rate limit middleware.
    """
    user: BaseUser = scope["user"]
    if "user" in scope:
        id_ = user.identity
        if id_:
            return id_, "default"
    raise EmptyInformation(scope)