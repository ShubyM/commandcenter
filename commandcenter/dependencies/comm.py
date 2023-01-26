from commandcenter.comm.providers.telalert import TelAlertClient
from commandcenter.setup.comm.telalert import setup_telalert_client



async def get_telalert_client() -> TelAlertClient:
    """Dependency for retrieving a TelAlert client."""
    return setup_telalert_client()