from typing import List

from commandcenter.api.setup.comm.telalert import setup_telalert_client
from commandcenter.comm.providers.telalert import TelAlertClient, TelAlertMessage



async def get_telalert_client() -> TelAlertClient:
    """Dependency for retrieving a TelAlert client."""
    return setup_telalert_client()


async def telalert_message(
    msg: str,
    groups: List[str] | None,
    destinations: List[str] | None,
    subject: str | None
) -> TelAlertMessage:
    """TelAlertMessage depenedency."""
    return TelAlertMessage(
        msg=msg,
        groups=groups,
        destinations=destinations,
        subject=subject
    )