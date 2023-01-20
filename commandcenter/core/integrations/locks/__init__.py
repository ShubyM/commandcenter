from commandcenter.core.util.enums import ObjSelection


# TODO: Implement Redis and Memcached locks
class AvailableLocks(ObjSelection):
    DEFAULT = "default", None