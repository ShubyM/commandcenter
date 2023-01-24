import os
import pathlib

from starlette.config import Config

from commandcenter.core.integrations.util import TIMEZONE



config = Config(".env")


CC_DEBUG_MODE = config(
    "CC_DEBUG_MODE",
    cast=bool,
    default=False
)
CC_HOME = config(
    "CC_HOME",
    cast=pathlib.Path,
    default=pathlib.Path("~").expanduser() / ".commandcenter"
)
CC_TIMEZONE = config(
    "CC_TIMEZONE",
    default=TIMEZONE
)
CC_OBJCACHE_DIR = config(
    "CC_OBJCACHE_DIR",
    cast=pathlib.Path,
    default=CC_HOME.joinpath("./.objcache")
)

os.makedirs(CC_HOME, exist_ok=True)