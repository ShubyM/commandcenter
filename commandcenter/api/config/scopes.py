import itertools

from starlette.config import Config
from starlette.datastructures import CommaSeparatedStrings

from commandcenter.auth.models import BaseUser



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

# TelAlert
CC_SCOPES_TELALERT_ACCESS = config(
    "CC_SCOPES_TELALERT_ACCESS",
    cast=CommaSeparatedStrings,
    default=""
)
CC_SCOPES_TELALERT_ALLOW_ANY = config(
    "CC_SCOPES_TELALERT_ALLOW_ANY",
    cast=bool,
    default=False
)
CC_SCOPES_TELALERT_RAISE_ON_NONE = config(
    "CC_SCOPES_TELALERT_RAISE_ON_NONE",
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

CC_SCOPES_COMMANDCENTER_VIEWER = config(
    "CC_SCOPES_COMMANDCENTER_VIEWER",
    default=""
)
CC_SCOPES_COMMANDCENTER_DEVELOPER = config(
    "CC_SCOPES_COMMANDCENTER_DEVELOPER",
    default=""
)
CC_SCOPES_COMMANDCENTER_ADMIN = config(
    "CC_SCOPES_COMMANDCENTER_ADMIN",
    default=""
)

# Read privelages
COMMANDCENTER_READ_ACCESS = [scope for scope in
    (
        CC_SCOPES_COMMANDCENTER_VIEWER,
        CC_SCOPES_COMMANDCENTER_DEVELOPER,
        CC_SCOPES_COMMANDCENTER_ADMIN
    )
]
COMMANDCENTER_READ_ALLOW_ANY = True
COMMANDCENTER_READ_RAISE_ON_NONE = False

# Write privelages
COMMANDCENTER_WRITE_ACCESS = [scope for scope in
    (
        CC_SCOPES_COMMANDCENTER_DEVELOPER,
        CC_SCOPES_COMMANDCENTER_ADMIN
    )
]
COMMANDCENTER_WRITE_ALLOW_ANY = True
COMMANDCENTER_WRITE_RAISE_ON_NONE = True

# Admin privelages
COMMANDCENTER_ADMIN_ACCESS = [scope for scope in (CC_SCOPES_COMMANDCENTER_ADMIN,)]
COMMANDCENTER_ADMIN_ALLOW_ANY = False
COMMANDCENTER_WRITE_RAISE_ON_NONE = True


ADMIN_USER = BaseUser(
    username="admin",
    first_name="Christopher",
    last_name="Newville",
    email="chrisnewville1396@gmail.com",
    upi=2191996,
    company="Prestige Worldwide",
    scopes=set(
        itertools.chain(
            list(CC_SCOPES_ADMIN_ACCESS),
            list(CC_SCOPES_PIWEB_ACCESS),
            list(CC_SCOPES_TRAXX_ACCESS),
            list(CC_SCOPES_TELALERT_ACCESS),
            COMMANDCENTER_READ_ACCESS,
            COMMANDCENTER_WRITE_ACCESS,
            COMMANDCENTER_ADMIN_ACCESS
        )
    )
)