import logging

from commandcenter.core.util.context import ip_address_context, user_context



class IPAddressFilter(logging.Filter):
    """Logging filter that adds the client IP Address to each log record."""
    def filter(self, record: logging.LogRecord) -> bool:
        ip_address = ip_address_context.get()
        if ip_address is not None:
            record.ip_address = ip_address
        return super().filter(record)


class UserFilter(logging.Filter):
    """Logging filter that adds the client username to each log record."""
    def filter(self, record: logging.LogRecord) -> bool:
        user = user_context.get()
        if user is not None:
            record.username = user.identity
        return super().filter(record)