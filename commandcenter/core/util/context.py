from contextvars import ContextVar
from typing import Optional

from commandcenter.core.auth import BaseUser
from commandcenter.core.sources import AvailableSources



ip_address_context: ContextVar[Optional[str]] = ContextVar("ip_address_context", default=None)
user_context: ContextVar[Optional[BaseUser]] = ContextVar("user_context", default=None)
source_context: ContextVar[Optional[AvailableSources]] = ContextVar("source_context", default=None)