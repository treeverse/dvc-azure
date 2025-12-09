import pytest

from dvc.testing.fixtures import *  # noqa: F403

from .fixtures import *  # noqa: F403


def pytest_addoption(parser):
    parser.addoption("--host", action="store", help="Host running azurite.")


@pytest.fixture(scope="session")
def host(request):
    return request.config.getoption("--host")
