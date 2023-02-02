from commandcenter.caching import singleton
from commandcenter.exceptions import NotConfigured



@singleton
def setup_telalert_client():
    """Setup TelAlert client from the runtime configuration."""
    from commandcenter.comm.providers.telalert import TelAlertClient
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