from commandcenter.core.util.enums import ObjSelection
from .local import LocalTimeseriesCollection



__all__ = [
    "LocalTimeseriesCollection",
]


class AvailableTimeseriesCollections(ObjSelection):
    LOCAL = "local", LocalTimeseriesCollection