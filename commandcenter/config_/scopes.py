from starlette.config import Config
from starlette.datastructures import CommaSeparatedStrings



config = Config(".env")


# PI Web
CC_SCOPES_PIWEB_ACCESS = config(
    "CC_SCOPES_PIWEB_ACCESS",
    cast=CommaSeparatedStrings,
    default=""
)
CC_SCOPES_PIWEB_ALLOW_ANY = config(
    "CC_SCOPES_PIWEB_ALLOW_ANY",
    cast=bool,
    default=False
)
CC_SCOPES_PIWEB_RAISE_ON_NONE = config(
    "CC_SCOPES_PIWEB_RAISE_ON_NONE",
    cast=bool,
    default=False
)

# Traxx
CC_SCOPES_TRAXX_ACCESS = config(
    "CC_SCOPES_TRAXX_ACCESS",
    cast=CommaSeparatedStrings,
    default=""
)
CC_SCOPES_TRAXX_ALLOW_ANY = config(
    "CC_SCOPES_TRAXX_ALLOW_ANY",
    cast=bool,
    default=False
)
CC_SCOPES_TRAXX_RAISE_ON_NONE = config(
    "CC_SCOPES_TRAXX_RAISE_ON_NONE",
    cast=bool,
    default=False
)

# Admin
CC_SCOPES_ADMIN_ACCESS = config(
    "CC_SCOPES_ADMIN_ACCESS",
    cast=CommaSeparatedStrings,
    default=""
)
CC_SCOPES_ADMIN_ALLOW_ANY = config(
    "CC_SCOPES_ADMIN_ALLOW_ANY",
    cast=bool,
    default=False
)