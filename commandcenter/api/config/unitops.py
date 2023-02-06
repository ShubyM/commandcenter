from starlette.config import Config

from commandcenter.__version__ import __title__ as DATABASE_NAME



config = Config(".env")


CC_UNITOPS_DATABASE_NAME = config(
    "CC_UNITOPS_DATABASE_NAME",
    default=DATABASE_NAME
)
CC_UNITOPS_COLLECTION_NAME = config(
    "CC_UNITOPS_COLLECTION_NAME",
    default="unitops"
)