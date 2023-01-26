import logging

from starlette.config import Config
from starlette.datastructures import CommaSeparatedStrings

from commandcenter.util import cast_logging_level


config = Config(".env")


SENTRY_ENABLED = config(
    "SENTRY_ENABLED",
    cast=bool,
    default=False
)
SENTRY_DSN = config(
    "SENTRY_DSN",
    default=""
)
SENTRY_TRACES_SAMPLE_RATE = config(
    "SENTRY_TRACES_SAMPLE_RATE",
    cast=float,
    default=1.0
)
SENTRY_LOGGING_LEVEL = config(
    "SENTRY_LOGGING_LEVEL",
    cast=cast_logging_level,
    default=logging.INFO
)
SENTRY_EVENT_LEVEL = config(
    "SENTRY_EVENT_LEVEL",
    cast=cast_logging_level,
    default=logging.ERROR
)
SENTRY_SEND_DEFAULT_PII = config(
    "SENTRY_SEND_DEFAULT_PII",
    cast=bool,
    default=False
)
SENTRY_IGNORE_LOGGERS = config(
    "SENTRY_IGNORE_LOGGERS",
    cast=CommaSeparatedStrings,
    default=""
)
SENTRY_ENVIRONMENT = config(
    "SENTRY_ENVIRONMENT",
    default="production"
)
SENTRY_SAMPLE_RATE = config(
    "SENTRY_SAMPLE_RATE",
    cast=float,
    default=1.0
)
SENTRY_MAX_BREADCRUMBS = config(
    "SENTRY_MAX_BREADCRUMBS",
    cast=int,
    default=100
)