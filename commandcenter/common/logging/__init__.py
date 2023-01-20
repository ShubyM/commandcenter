import logging
import logging.config
import pathlib

import yaml

from commandcenter.config.logging import CC_LOGGING_CONFIG_PATH
from commandcenter.config.logging.sentry import (
    SENTRY_DSN,
    SENTRY_ENABLED,
    SENTRY_ENVIRONMENT,
    SENTRY_EVENT_LEVEL,
    SENTRY_IGNORE_LOGGERS,
    SENTRY_LOGGING_LEVEL,
    SENTRY_MAX_BREADCRUMBS,
    SENTRY_SAMPLE_RATE,
    SENTRY_SEND_DEFAULT_PII,
    SENTRY_TRACES_SAMPLE_RATE
)


# This path will be used if `CC_LOGGING_CONFIG_PATH` is None
DEFAULT_LOGGING_SETTINGS_PATH = pathlib.Path(__file__).parent / "logging.yml"


def setup_logging() -> None:
    """Sets up logging for this runtime."""
    # If the user has specified a logging path and it exists we will ignore the
    # default entirely rather than dealing with complex merging
    path = (
        CC_LOGGING_CONFIG_PATH
        if CC_LOGGING_CONFIG_PATH is not None and CC_LOGGING_CONFIG_PATH.exists()
        else DEFAULT_LOGGING_SETTINGS_PATH
    )
    config = yaml.safe_load(path.read_text())
    logging.config.dictConfig(config)


def setup_sentry() -> None:
    """Sets up sentry SDK for application."""
    try:
        import sentry_sdk
    except ImportError:
        return
    if SENTRY_ENABLED and SENTRY_DSN:
        integrations = []
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.logging import LoggingIntegration, ignore_logger
        from sentry_sdk.integrations.starlette import StarletteIntegration
        
        integrations.extend(
            [
                FastApiIntegration(),
                LoggingIntegration(
                    level=SENTRY_LOGGING_LEVEL,
                    event_level=SENTRY_EVENT_LEVEL
                ),
                StarletteIntegration()
            ]
        )
        try:
            import redis
        except ImportError:
            pass
        else:
            from sentry_sdk.integrations.redis import RedisIntegration
            integrations.append(RedisIntegration())
        
        sentry_sdk.init(
            dsn=SENTRY_DSN,
            traces_sample_rate=SENTRY_TRACES_SAMPLE_RATE,
            integrations=integrations,
            send_default_pii=SENTRY_SEND_DEFAULT_PII,
            sample_rate=SENTRY_SAMPLE_RATE,
            environment=SENTRY_ENVIRONMENT,
            max_breadcrumbs=SENTRY_MAX_BREADCRUMBS
        )
        if SENTRY_IGNORE_LOGGERS:
            loggers = list(SENTRY_IGNORE_LOGGERS)
            for logger in loggers:
                ignore_logger(logger)