import logging
import socket

from .context import IPAddressFilter, UserFilter



__all__ = [
    "IPAddressFilter",
    "UserFilter"
]



HOST = socket.getaddrinfo(socket.gethostname(), 0, flags=socket.AI_CANONNAME)[0][3]


class HostFilter(logging.Filter):
    """Logging filter that adds the host to each log record."""
    def filter(self, record: logging.LogRecord) -> bool:
        record.host = HOST
        return True