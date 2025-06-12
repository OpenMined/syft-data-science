from pathlib import Path

import pytest
from syft_core import Client as SyftBoxClient
from syft_core import SyftClientConfig
from syft_event import SyftEvents
from syft_rds.client.rds_client import RDSClient, init_session
from syft_rds.orchestra import setup_rds_stack
from syft_rds.server.app import create_app
from syft_rds.store import YAMLStore

from tests.mocks import MockUserSchema

DO_EMAIL = "data_owner@test.openmined.org"
DS_EMAIL = "data_scientist@test.openmined.org"

# NOTE: for testing real RPC and file sharing without launching the full stack, we use a shared data dir.
SHARED_DATA_DIR = "shared_data_dir"

# paths to test assets
TEST_DIR = Path(__file__).parent
ASSET_PATH = TEST_DIR / "assets"
DO_PATH = ASSET_PATH / "do"
PRIVATE_DATA_PATH = DO_PATH / "private"
MOCK_DATA_PATH = DO_PATH / "mock"
PRIVATE_CODE_PATH = DO_PATH / "private_code"
MOCK_CODE_PATH = DO_PATH / "mock_code"
README_PATH = DO_PATH / "README.md"
DO_OUTPUT_PATH = DO_PATH / "job_outputs"
DS_PATH = ASSET_PATH / "ds"
TEST_DOCKERFILE_FILE_PATH = TEST_DIR.parent / "runtimes" / "python_test.Dockerfile"


@pytest.fixture
def do_syftbox_client(tmp_path: Path) -> SyftBoxClient:
    return SyftBoxClient(
        SyftClientConfig(
            email=DO_EMAIL,
            client_url="http://localhost:5000",
            path=tmp_path / "syftbox_client_config.json",
            data_dir=tmp_path / "clients" / SHARED_DATA_DIR,
        ),
    )


@pytest.fixture
def ds_syftbox_client(tmp_path: Path) -> SyftBoxClient:
    return SyftBoxClient(
        SyftClientConfig(
            email=DS_EMAIL,
            client_url="http://localhost:5001",
            path=tmp_path / "syftbox_client_config.json",
            data_dir=tmp_path / "clients" / SHARED_DATA_DIR,
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
def tmp_store_dir(tmp_path):
    """Fixture for creating a temporary database directory."""
    return tmp_path / "store"


@pytest.fixture
def yaml_store(tmp_store_dir):
    """Fixture for initializing the YAML store."""

    def _create_yaml_store(schema):
        return YAMLStore(item_type=schema, store_dir=tmp_store_dir)

    return _create_yaml_store


@pytest.fixture
def mock_user_store(yaml_store) -> YAMLStore:
    return yaml_store(MockUserSchema)


@pytest.fixture
def mock_user_1():
    return MockUserSchema(name="Alice", email="alice@openmined.org")


@pytest.fixture
def mock_user_2():
    return MockUserSchema(name="Bob", email="bob@openmined.org")


@pytest.fixture
def rds_no_sync_stack(tmp_path):
    """
    Setup full RDS stack in memory, with shared data dir. This means:
    - Real, file-based RPC
    - No file sync (files are shared by default)
    - Sync permissions are ignored
    """
    stack = setup_rds_stack(
        root_dir=tmp_path,
        reset=True,
        log_level="DEBUG",
        rpc_expiry="2s",
    )

    yield stack
    stack.stop()
