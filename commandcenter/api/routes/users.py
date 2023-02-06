from fastapi import APIRouter, Depends, Request

from commandcenter.auth import BaseUser
from commandcenter.api.dependencies import requires



router = APIRouter(prefix="/users", tags=["Users"])


@router.get("/whoami", response_model=BaseUser, dependencies=[Depends(requires())])
async def get_user(request: Request) -> BaseUser:
    """Retrieve user information for current logged in user."""
    return request.user.dict()