import pytest
from runtime.bootstrap import start_platform, stop_platform

@pytest.fixture(scope="session", autouse=True)
def ray_platform():
    start_platform()
    yield
    stop_platform()