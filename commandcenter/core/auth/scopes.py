from typing import Sequence

from fastapi import HTTPException, status
from fastapi.requests import Request
from starlette.authentication import BaseUser

from commandcenter.exceptions import NotConfigured



class requires:
    """Class based dependency for authorization.
    
    The `AuthenticationMiddleware` must be installed in order to use this class
    but it will work with any backend.

    Args:
        scopes: The required scopes to access the resource
        any_: If `True` and the user has any of the scopes, authorization will
            succeed. Otherwise, the user will need all scopes.
        raise_on_no_scopes: If `True` a `NotConfigured` error will be raised
            when an endpoint with no required scopes is hit.

    Examples:
    >>> router = APIRouter(prefix="/admin", dependencies=[Depends(requires(["ADMIN"]))])
    ... # The most common use case is to define scopes for an entire route
    ... # assuming that route is accessing similarly authorized resources

    >>> # You can also layer scopes down to the endpoint
    >>> @router.post("/changepassword", dependencies=[Depends(requires(["SUPERADMIN"]))])
    ... async def change_password(...):
    ...     # You now need to have ADMIN and SUPERADMIN privelages to access this
    ...     ...
    
    >>> # You can require that a user have all or any of the scopes to access
    ... # with the 'any_' argument. By default, they must have all scopes
    >>> @router.get("/useraccounts", dependencies=[Depends(requires(["VIEWUSER", "MODIFYUSER"], any_=True))])
    ... async def get_user(...):
    ...     # You need to have ADMIN and one of VIEWUSER, MODIFYUSER to access this
    ...     ...

    >>> # If no scopes are provided, the user must be authenticated but they
    ... # do not require any specific scopes to access the resource.
    >>> router_unprotected = APIRouter(prefix="/user", dependencies=[Depends(requires())])
    ... @router_unprotected.get("/me")
    ... async def get_me(...):
    ...     # The user only need be authenticated (valid user) to access this resource
    ...     ...
    >>> # Unless `raise_on_no_scopes` = `True`, then providing no scopes will
    ... # raise an error
    >>> @router.get("/usercreds", dependencies=[Depends(requires(raise_on_no_scopes=True))])
    ... async def get_creds(...):
    ...     # This will raise a `NotConfigured` error because no scopes were
    ...     # provided to the dependency
    ...     ...
    """
    def __init__(
        self,
        scopes: Sequence[str] = [],
        any_: bool = False,
        raise_on_no_scopes: bool = False
    ) -> None:
        self.scopes = set(scopes)
        self.any = any_
        self.raise_on_no_scopes = raise_on_no_scopes

    async def __call__(self, request: Request) -> None:
        self.authenticate_user(request)
        self.authorize_user(request)
    
    def authenticate_user(self, request: Request) -> None:
        """Verify user is authenticated.
        
        Raises:
            HTTPException: 401 Unauthorized.
        """
        user: BaseUser = request.user
        if not user.is_authenticated:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not Authenticated."
            )

    def authorize_user(self, request: Request) -> None:
        """Verify user scopes against required scopes.
        
        Raises:
            HTTPException: 401 Unauthorized.
        """
        if not self.scopes:
            if self.raise_on_no_scopes:
                raise NotConfigured("No scopes specified for this endpoint.")
            return
        scopes = request.auth
        if self.any and any([permission in scopes for permission in self.scopes]):
            return
        elif all([permission in scopes for permission in self.scopes]):
            return
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User does not have the required scopes to access this resource."
            )