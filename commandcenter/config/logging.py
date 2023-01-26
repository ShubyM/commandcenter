from starlette.config import Config

from commandcenter.util import cast_path



config = Config(".env")


CC_LOGGING_CONFIG_PATH = config(
    "CC_LOGGING_CONFIG_PATH",
    cast=cast_path,
    default=""
)