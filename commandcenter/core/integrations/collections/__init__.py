from commandcenter.core.util.enums import ObjSelection
from .local import LocalTimeseriesCollection



__all__ = [
    "LocalTimeseriesCollection",
]

# TODO: Implement RedisTimeseries collection
class AvailableTimeseriesCollections(ObjSelection):
    DEFAULT = "default", LocalTimeseriesCollection
    LOCAL = "local", LocalTimeseriesCollection