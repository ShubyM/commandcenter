from enum import Enum
from typing import Any, Type



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