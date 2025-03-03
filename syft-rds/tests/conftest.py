from pathlib import Path

import pytest
from syft_core import Client as SyftBoxClient
from syft_core import SyftClientConfig
from syft_event import SyftEvents
from syft_rds.client.rds_client import RDSClient, init_session
from syft_rds.models.base import BaseSchema
from syft_rds.server.app import create_app
from syft_rds.server.routers.job_router import job_store
from syft_rds.server.routers.runtime_router import runtime_store
from syft_rds.server.routers.user_code_router import user_code_store
from syft_rds.store import YAMLFileSystemDatabase

from tests.mocks import MockUserSchema

DO_EMAIL = "data_owner@test.openmined.org"
DS_EMAIL = "data_scientist@test.openmined.org"


@pytest.fixture(autouse=True)
def reset_state():
    """Reset all internal state between tests"""
    # Clear all stores
    job_store.clear()
    user_code_store.clear()
    runtime_store.clear()

    # Reset the private attribute to a new empty dict
    BaseSchema.__private_attributes__["_client_cache"].default = dict()
    yield


@pytest.fixture
def do_syftbox_client(tmp_path) -> SyftBoxClient:
    return SyftBoxClient(
        SyftClientConfig(
            email=DO_EMAIL,
            client_url="http://localhost:5000",
            path=tmp_path / "syftbox_client_config.json",
        ),
    )


@pytest.fixture
def ds_syftbox_client(tmp_path) -> SyftBoxClient:
    return SyftBoxClient(
        SyftClientConfig(
            email=DS_EMAIL,
            client_url="http://localhost:5001",
            path=tmp_path / "syftbox_client_config.json",
        ),
    )


@pytest.fixture
def rds_server(do_syftbox_client: SyftBoxClient):
    return create_app(do_syftbox_client)


@pytest.fixture
def ds_rds_client(
    rds_server: SyftEvents, ds_syftbox_client: SyftBoxClient
) -> RDSClient:
    return init_session(
        DO_EMAIL,
        syftbox_client=ds_syftbox_client,
        mock_server=rds_server,
    )


@pytest.fixture
def do_rds_client(
    rds_server: SyftEvents, do_syftbox_client: SyftBoxClient
) -> RDSClient:
    return init_session(
        DO_EMAIL,
        syftbox_client=do_syftbox_client,
        mock_server=rds_server,
    )


@pytest.fixture()
def temp_db_path(tmp_path):
    """Fixture for creating a temporary database directory."""
    return tmp_path / "db"


@pytest.fixture
def yaml_store(temp_db_path):
    """Fixture for initializing the YAML store."""

    def _create_yaml_store(schema):
        return YAMLFileSystemDatabase(schema=schema, db_path=temp_db_path)

    return _create_yaml_store


@pytest.fixture
def mock_user_store(yaml_store) -> YAMLFileSystemDatabase:
    return yaml_store(MockUserSchema)


@pytest.fixture
def mock_user_1():
    return MockUserSchema(name="Alice", email="alice@openmined.org")


@pytest.fixture
def mock_user_2():
    return MockUserSchema(name="Bob", email="bob@openmined.org")


@pytest.fixture
def do_syftbox_config(tmp_path) -> SyftClientConfig:
    do_email = "data_owner@openmined.org"
    config_path = Path(tmp_path) / do_email / "config.json"
    data_dir = Path(tmp_path) / do_email
    conf = SyftClientConfig(
        path=config_path,
        data_dir=data_dir,
        email=do_email,
        client_url="http://test:8080",
    )
    conf.data_dir.mkdir(parents=True, exist_ok=True)
    conf.save()
    return conf


@pytest.fixture
def ds_syftbox_config(tmp_path) -> SyftClientConfig:
    ds_email = "data_scientist@openmined.org"
    config_path = Path(tmp_path) / ds_email / "config.json"
    data_dir = Path(tmp_path) / ds_email
    conf = SyftClientConfig(
        path=config_path,
        data_dir=data_dir,
        email=ds_email,
        client_url="http://test:8081",
    )
    conf.data_dir.mkdir(parents=True, exist_ok=True)
    conf.save()
    return conf
