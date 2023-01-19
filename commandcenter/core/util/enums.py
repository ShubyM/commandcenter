from enum import Enum
from typing import TypeVar



T = TypeVar("T")


class ObjSelection(str, Enum):
    """Define a selection of objects.
    
    Examples:
    >>> class AuthBackends(ObjSelection):
    ...     AD = "ad", ActiveDirectoryBackend
    Now we can select a backend by a key
    >>> backend = AuthBackends("ad").cls
    This is particularly useful in confguration
    >>> config = Config(".env")
    ... BACKEND = config("BACKEND", cast=lambda v: AuthBackends(v).cls, default=AuthBackends.default.value)
    """
    def __new__(cls, value: str, type_: T) -> "ObjSelection":
        obj = str.__new__(cls, value)
        obj._value_ = value
        
        obj.cls = type_  # type: ignore[attr-defined]
        return obj


class LogLevels(str, Enum):
    def __new__(cls, value: str, intrep: int) -> "LogLevels":
        obj = str.__new__(cls, value)
        obj._value_ = value

        obj.intrep = intrep  # type: ignore[attr-defined]
        return obj
    
    @classmethod
    def get_intrep(cls, level: str) -> int:
        try:
            return cls(level.upper()).intrep
        except (ValueError, AttributeError):
            return cls.NOTSET.intrep
        
    NOTSET = "NOTSET", 20
    DEBUG = "DEBUG", 10
    INFO = "INFO", 20
    WARNING = "WARNING", 30
    ERROR = "ERROR", 40
    CRITICAL = "CRITICAL", 50