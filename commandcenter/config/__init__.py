import pathlib

from starlette.config import Config

from commandcenter.util import TIMEZONE



config = Config(".env")


CC_HOME = config(
    "CC_HOME",
    cast=pathlib.Path,
    default=pathlib.Path("~").expanduser() / ".commandcenter"
)
CC_DEBUG_MODE = config(
    "CC_DEBUG_MODE",
    cast=bool,
    default=False
)
CC_INTERACTIVE_AUTH = config(
    "CC_INTERACTIVE_AUTH",
    cast=bool,
    default=False
)
CC_TIMEZONE = config(
    "CC_TIMEZONE",
    default=TIMEZONE
)
CC_CACHE_DIR = config(
    "CC_CACHE_DIR",
    cast=pathlib.Path,
    default=CC_HOME.joinpath("./.cache")
)