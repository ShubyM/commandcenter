import pathlib

from starlette.config import Config

from commandcenter.config import CC_HOME
from commandcenter.caching import Caches



config = Config(".env")


CC_CACHE_DIR = config(
    "CC_CACHE_DIR",
    cast=pathlib.Path,
    default=CC_HOME.joinpath("./.cache")
)
CC_CACHE_BACKEND = config(
    "CC_CACHE_BACKEND",
    cast=lambda v: Caches(v).cls,
    default=Caches.MEMORY.value
)