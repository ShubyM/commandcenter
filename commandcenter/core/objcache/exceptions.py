import types
from typing import Any, Optional

from commandcenter.core.objcache.util import (
    get_cached_func_name,
    get_fqn_type,
    get_return_value_type
)
from commandcenter.exceptions import CommandCenterException



class ObjCacheException(CommandCenterException):
    """Base obj cache exception that all exceptions raised by cache decorators
    derive from.
    """


class UnhashableTypeError(ObjCacheException):
    """Internal exception raised when a function argument is not hashable."""


class CacheKeyNotFoundError(ObjCacheException):
    """The hash result of the inputs does not match the key in a cache result.
    
    This normally leads to a cache miss and is not propagated.
    """


class CacheError(ObjCacheException):
    """Raised by the `memo` decorator when an object cannot be pickled or `memo`
    cannot read/write from/to the disk.
    """


class UnhashableParamError(ObjCacheException):
    """Raised when an input value to a caching function is not hashable.
    
    This can be avoided by attaching a leading underscore (_) to the argument.
    The argument will be skipped when calculating the cache key.
    """
    def __init__(
        self,
        func: types.FunctionType,
        arg_name: Optional[str],
        arg_value: Any,
        orig_exc: BaseException,
    ):
        msg = self._create_message(func, arg_name, arg_value)
        super().__init__(msg)
        self.with_traceback(orig_exc.__traceback__)

    @staticmethod
    def _create_message(
        func: types.FunctionType,
        arg_name: Optional[str],
        arg_value: Any,
    ) -> str:
        arg_name_str = arg_name if arg_name is not None else "(unnamed)"
        arg_type = get_fqn_type(arg_value)
        func_name = func.__name__
        arg_replacement_name = f"_{arg_name}" if arg_name is not None else "_arg"

        return " ".join(
            [
                segment.strip() for segment in
                """Cannot hash argument '{}' (of type {}) in '{}'. To address this,
                you can force this argument to be ignored by adding a leading
                underscore to the arguments name in the function signature (eg. '{}')
                """.format(
                    arg_name_str,
                    arg_type,
                    func_name,
                    arg_replacement_name
                ).splitlines()
            ]
        )


class UnserializableReturnValueError(ObjCacheException):
    """Raised when a return value from a function cannot be serialized with pickle."""
    def __init__(self, func: types.FunctionType, return_value: types.FunctionType):
        msg = self._create_message(func, return_value)
        super().__init__(msg)
    
    def _create_message(self, func, return_value) -> str:
        return " ".join(
            [
                segment.strip() for segment in
                """Cannot serialize the return value of type '{}' in '{}'. 'memo'
                uses pickle to serialize the functions return value and safely store it in
                cache without mutating the original object. Please convert the return value
                to a pickle-serializable type. If you want to cache unserializable objects
                such as database connections or HTTP sessions, use 'singleton' instead.
                """.format(
                    get_return_value_type(return_value),
                    get_cached_func_name(func)
                ).splitlines()
            ]
        )