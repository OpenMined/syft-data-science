from syft_rds.client.rds_client import RDSClient
from syft_rds.models.models import Dataset
from tests.conftest import (
    MOCK_CODE_PATH,
    MOCK_DATA_PATH,
    PRIVATE_CODE_PATH,
    PRIVATE_DATA_PATH,
    README_PATH,
)


def create_dataset(do_rds_client: RDSClient, name: str) -> Dataset:
    data = do_rds_client.dataset.create(
        name=name,
        path=PRIVATE_DATA_PATH,
        mock_path=MOCK_DATA_PATH,
        summary="Test data",
        description_path=README_PATH,
        tags=["test"],
    )
    return data


def create_dataset_with_custom_runtime(do_rds_client: RDSClient, name: str) -> Dataset:
    code_path = (PRIVATE_CODE_PATH / "do.py").as_posix()
    runtime = do_rds_client.runtime.create(
        name="test",
        kind="python",
        config={"cmd": ["python", code_path]},
    )
    data = do_rds_client.dataset.create(
        name=name,
        path=PRIVATE_CODE_PATH,
        mock_path=MOCK_CODE_PATH,
        description_path=README_PATH,
        runtime_id=runtime.uid,
    )
    return data
