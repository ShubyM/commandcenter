from commandcenter.auth.backends.activedirectory import ActiveDirectoryBackend
from commandcenter.util import ObjSelection



__all__ = ["AuthBackends"]


class AuthBackends(ObjSelection):
    DEFAULT = "default", ActiveDirectoryBackend
    ACTIVE_DIRECTORY = "activedirectory", ActiveDirectoryBackend