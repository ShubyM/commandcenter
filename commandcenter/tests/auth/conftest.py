
import os
import pytest
import functools

from xprocess import ProcessStarter
from fastapi.testclient import TestClient



@pytest.fixture
def test_client_factory(anyio_backend_name, anyio_backend_options) -> TestClient:
    # anyio_backend_name defined by:
    # https://anyio.readthedocs.io/en/stable/testing.html#specifying-the-backends-to-run-on
    return functools.partial(
        TestClient,
        backend=anyio_backend_name,
        backend_options=anyio_backend_options
    )
    # return TestClient



@pytest.fixture(scope="session")
def ldap_server(xprocess):
    class Starter(ProcessStarter):
        timeout = 5
        pattern = "started on port 3004"

        popen_kwargs = {
            "shell": True,
            "universal_newlines": True,
        }

        cwd = os.path.dirname(os.path.realpath(__file__))

        config = os.path.join(cwd, 'config', 'server-conf.json')
        db = os.path.join(cwd, 'config', 'db.json')
        
        args = ['npx', 'ldap-server-mock', f'--conf={config}', f'--database={db}']

    # ensure process is running and return its logfile
    xprocess.ensure("ldap-server", Starter)
    yield
    # clean up whole process tree afterwards
    xprocess.getinfo("ldap-server").terminate()




