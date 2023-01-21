"""
Object cache management tools for commandcenter.

The cahing API was heavily influenced and adapted from
[Streamlit](https://docs.streamlit.io/library/advanced-features/experimental-cache-primitives).
It has been adapted to work with async methods and context variables but the
underlying API from streamlit is more or less unchanged.
"""

from .exceptions import (
    CacheKeyNotFoundError,
    ObjCacheException,
    UnhashableParamError,
    UnhashableTypeError
)
from .memo import memo, set_cache_dir
from .singleton import iter_singletons, singleton



__all__ = [
    "CacheKeyNotFoundError",
    "ObjCacheException",
    "UnhashableParamError",
    "UnhashableTypeError",
    "memo",
    "set_cache_dir",
    "iter_singletons",
    "singleton",
]