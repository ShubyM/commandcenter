from datetime import datetime, timedelta

import dateutil.parser
from fastapi import HTTPException, Query, Request, status

from commandcenter.util import FileWriter, get_file_format_writer



def get_file_writer(request: Request) -> FileWriter:
    """Returns a writer/buffer/suffix combo for streaming files.
    
    Defaults to csv writer if "accept" isnt present.
    """
    accept = request.headers.get("accept", "text/csv")
    try:
        return get_file_format_writer(accept)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE)


def parse_timestamp(query: Query, default_timedelta: timedelta | None = None):
    """Parse a str timestamp from a request."""
    default_timedelta = default_timedelta or timedelta(seconds=0)
    def wrapper(time: str | None = query) -> datetime:
        now = datetime.now()
        if not time:
            return now - default_timedelta
        try:
            return dateutil.parser.parse(time)
        except dateutil.parser.ParserError:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)
    return wrapper