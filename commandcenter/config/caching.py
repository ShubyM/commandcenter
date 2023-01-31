import pathlib

from starlette.config import Config

from commandcenter.caching.memo import caches
from commandcenter.config import CC_HOME



config = Config(".env")


CC_CACHE_DIR = config(
    "CC_CACHE_DIR",
    cast=pathlib.Path,
    default=CC_HOME.joinpath("./.cache")
)
CC_CACHE_BACKEND = config(
    "CC_CACHE_BACKEND",
    cast=lambda v: caches(v).cls,
    default=caches.MEMORY.value
)