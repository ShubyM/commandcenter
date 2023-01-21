from .admin import router as admin_
from .users import router as users_


# TODO:
# - Systems stats routes
# - Logging config routes
# - Env config routes
__all__ = [
    "admin_",
    "users_",
]