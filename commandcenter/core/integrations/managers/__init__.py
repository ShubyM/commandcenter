from commandcenter.core.util.enums import ObjSelection
from .local import LocalManager



__all__ = [
    "LocalManager",
]


class AvailableManagers(ObjSelection):
    DEFAULT = "default", LocalManager
    LOCAL = "local", LocalManager