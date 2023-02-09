import pytest
from starlette.middleware import Middleware


from fastapi import Depends


from commandcenter.auth.debug import DebugAuthenticationMiddleware
from commandcenter.auth import AuthenticationClient
from commandcenter.api.config.scopes import ADMIN_USER
from commandcenter.api.setup.auth import setup_auth_backend


from commandcenter.api.dependencies import (
    get_auth_client
)




from fastapi.testclient import TestClient
from fastapi import FastAPI

@pytest.mark.asyncio
async def test_always_pass(test_client_factory):
    DebugAuthenticationMiddleware.set_user(ADMIN_USER)
    app: FastAPI = FastAPI(middleware=[Middleware(DebugAuthenticationMiddleware, backend=setup_auth_backend())])





    @app.post("/")
    async def auth(client: AuthenticationClient = Depends(get_auth_client)):
        # response = await client.authenticate("MISHRSX29", "")
        response = await client.get_user("MISHRSX29")
        return {"client": response}

    # create a client for the app using fixure
    client: TestClient = test_client_factory(app)


    # call an endpoint
    response = client.post("/")
    print(response.content)
    # sanity check
    assert response.status_code == 200

    








