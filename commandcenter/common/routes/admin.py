from fastapi import APIRouter, Depends

from commandcenter.dependencies import get_token_handler, requires
from commandcenter.common.models import Status
from commandcenter.config.scopes import CC_SCOPES_ADMIN_ACCESS, CC_SCOPES_ADMIN_ALLOW_ANY
from commandcenter.core.auth import TokenHandler



router = APIRouter(
    prefix="/admin",
    dependencies=[
        Depends(
            requires(
                scopes=list(CC_SCOPES_ADMIN_ACCESS),
                any_=CC_SCOPES_ADMIN_ALLOW_ANY,
                raise_on_no_scopes=True
            )
        )
    ],
    tags=["Admin"]
)


@router.patch("/tokens/invalidate", response_model=Status)
async def invalidate(handler: TokenHandler = Depends(get_token_handler)) -> Status:
    """Invalidate all client tokens by changing the API key."""
    handler.invalidate()
    return {"status": "OK"}