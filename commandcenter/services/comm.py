from commandcenter.core.objcache import singleton
from commandcenter.exceptions import NotConfigured



@singleton
async def get_telalert_client():
    """Retrieve the telalert client with environment settings."""
    from commandcenter.core.comm.providers.telalert import TelAlertClient
    from commandcenter.config.comm.providers.telalert import (
        CC_COMM_PROVIDERS_TELALERT_HOST,
        CC_COMM_PROVIDERS_TELALERT_MAX_CONCURRENCY,
        CC_COMM_PROVIDERS_TELALERT_PATH,
        CC_COMM_PROVIDERS_TELALERT_TIMEOUT
    )
    if not CC_COMM_PROVIDERS_TELALERT_PATH or not CC_COMM_PROVIDERS_TELALERT_HOST:
        raise NotConfigured("Telalert settings not configured.")
    try:
        return TelAlertClient(
            path=CC_COMM_PROVIDERS_TELALERT_PATH,
            host=CC_COMM_PROVIDERS_TELALERT_HOST,
            max_concurrency=CC_COMM_PROVIDERS_TELALERT_MAX_CONCURRENCY,
            timeout=CC_COMM_PROVIDERS_TELALERT_TIMEOUT
        )
    except FileNotFoundError as err:
        raise NotConfigured("Invalid telalert settings.") from err