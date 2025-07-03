from pathlib import Path

import pytest
from syft_core import Client as SyftBoxClient
from syft_core import SyftClientConfig
from syft_datasets.dataset_manager import SyftDatasetManager

ASSETS_DIR = Path(__file__).parent.parent / "assets"

MOCK_DATA_PATH = ASSETS_DIR / "mock.csv"
PRIVATE_DATA_PATH = ASSETS_DIR / "private.csv"
README_PATH = ASSETS_DIR / "README.md"


@pytest.fixture()
def do_config(tmp_path: Path) -> SyftClientConfig:
    cfg = SyftClientConfig(
        email="do@datasets.openmined.org",
        client_url="http://testserver:8000",  # Not used in tests.
        path=tmp_path / "do_config.json",
        data_dir=tmp_path,
    )
    cfg.save()
    return cfg


@pytest.fixture()
def do_syftbox_client(do_config: SyftClientConfig) -> SyftBoxClient:
    return SyftBoxClient(do_config)


@pytest.fixture()
def ds_config(tmp_path: Path) -> SyftClientConfig:
    cfg = SyftClientConfig(
        email="ds@datasets.openmined.org",
        client_url="http://testserver:8001",  # Not used in tests.
        path=tmp_path / "ds_config.json",
        data_dir=tmp_path,
    )
    cfg.save()
    return cfg


@pytest.fixture()
def ds_syftbox_client(ds_config: SyftClientConfig) -> SyftBoxClient:
    return SyftBoxClient(ds_config)


@pytest.fixture()
def dataset_manager(do_syftbox_client: SyftBoxClient) -> SyftDatasetManager:
    return SyftDatasetManager(syftbox_client=do_syftbox_client)


@pytest.fixture()
def ds_dataset_manager(ds_syftbox_client: SyftBoxClient) -> SyftDatasetManager:
    return SyftDatasetManager(syftbox_client=ds_syftbox_client)
