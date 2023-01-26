from contextlib import contextmanager
from contextvars import ContextVar
from typing import Generator

from commandcenter.auth.models import BaseUser
from commandcenter.sources import Sources



ip_address_context: ContextVar[str | None] = ContextVar("ip_address_context", default=None)
user_context: ContextVar[BaseUser | None] = ContextVar("user_context", default=None)
source_context: ContextVar[Sources | None] = ContextVar("source_context", default=None)



@contextmanager
def set_source(source: Sources) -> Generator[None, None, None]:
    """Context manager which sets the source context."""
    token = source_context.set(source)
    try:
        yield
    finally:
        source_context.reset(token)