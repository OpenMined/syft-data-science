import pytest
from syft_core import Client, SyftClientConfig
from syft_event import SyftEvents
from syft_rds.client.rds_client import RDSClient, init_session
from syft_rds.server.app import create_app
from syft_rds.server.routers.job_router import job_store
from syft_rds.server.routers.user_code_router import user_code_store
from syft_rds.server.routers.runtime_router import runtime_store
from syft_rds.server.routers.dataset_router import dataset_store
from syft_rds.models.base import ItemBase
from syft_rds.store import YAMLFileSystemDatabase
from tests.mocks import MockUserSpec

HOST_EMAIL = "alice@openmined.org"


@pytest.fixture(autouse=True)
def reset_state():
    """Reset all internal state between tests"""
    # Clear all stores
    job_store.items.clear()
    user_code_store.items.clear()
    runtime_store.items.clear()
    dataset_store.items.clear()

    # Reset the private attribute to a new empty dict
    ItemBase.__private_attributes__["_client_cache"].default = dict()
    yield


@pytest.fixture
def host_syftbox_client(tmp_path):
    return Client(
        SyftClientConfig(
            email=HOST_EMAIL,
            client_url="http://localhost:5000",
            path=tmp_path / "syftbox_client_config.json",
        ),
    )


@pytest.fixture
def rds_server(host_syftbox_client: Client):
    return create_app(host_syftbox_client)


@pytest.fixture
def rds_client(rds_server: SyftEvents) -> RDSClient:
    return init_session(HOST_EMAIL, mock_server=rds_server)


@pytest.fixture
def server_client(rds_server: SyftEvents) -> RDSClient:
    return init_session(HOST_EMAIL, mock_server=rds_server)



@pytest.fixture()
def temp_db_path(tmp_path):
    """Fixture for creating a temporary database directory."""
    return tmp_path / "db"

@pytest.fixture
def yaml_store(temp_db_path):
    """Fixture for initializing the YAML store."""
    def _create_yaml_store(spec):
        return YAMLFileSystemDatabase(spec=spec, db_path=temp_db_path)
    return _create_yaml_store


@pytest.fixture
def mock_user_store(yaml_store) -> YAMLFileSystemDatabase:
    return yaml_store(MockUserSpec)

@pytest.fixture
def mock_user_1():
    return MockUserSpec(name="Alice", email="alice@openmined.org")

@pytest.fixture
def mock_user_2():
    return MockUserSpec(name="Bob", email="bob@openmined.org")


