from fastapi import APIRouter, Depends
from fastapi.background import BackgroundTasks

from commandcenter.comm.providers.telalert import TelAlertClient, TelAlertMessage
from commandcenter.common.models import Status, StatusOptions
from commandcenter.config.scopes import (
    CC_SCOPES_TELALERT_ACCESS,
    CC_SCOPES_TELALERT_ALLOW_ANY,
    CC_SCOPES_TELALERT_RAISE_ON_NONE
)
from commandcenter.dependencies import (
    get_telalert_client,
    telalert_message,
    requires
)



router = APIRouter(
    prefix="/telalert",
    dependencies=[
        Depends(
            requires(
                scopes=list(CC_SCOPES_TELALERT_ACCESS),
                any_=CC_SCOPES_TELALERT_ALLOW_ANY,
                raise_on_no_scopes=CC_SCOPES_TELALERT_RAISE_ON_NONE
            )
        )
    ],
    tags=["TelAlert"]
)


@router.put("/send", response_model=Status)
async def send(
    tasks: BackgroundTasks,
    client: TelAlertClient = Depends(get_telalert_client),
    message: TelAlertMessage = Depends(telalert_message)
) -> Status:
    """Send a dial out message through the TelAlert system."""
    tasks.add_task(client.send_alert(message))
    return Status(status=StatusOptions.OK)
