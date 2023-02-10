

from commandcenter.config.sentry import (
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



def configure_sentry() -> None:
    """Configure sentry SDK integrations from the environment."""
    try:
        import sentry_sdk
    except ImportError:
        return
    if not SENTRY_ENABLED or not SENTRY_DSN:
        return
    
    integrations = []
    from sentry_sdk.integrations.fastapi import FastApiIntegration
    from sentry_sdk.integrations.logging import LoggingIntegration, ignore_logger
    from sentry_sdk.integrations.pymongo import PyMongoIntegration
    from sentry_sdk.integrations.starlette import StarletteIntegration
    
    integrations.extend(
        [
            FastApiIntegration(),
            LoggingIntegration(
                level=SENTRY_LOGGING_LEVEL,
                event_level=SENTRY_EVENT_LEVEL
            ),
            PyMongoIntegration(),
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