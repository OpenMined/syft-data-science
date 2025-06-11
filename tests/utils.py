from syft_rds.client.rds_client import RDSClient
from syft_rds.models.models import Dataset
from tests.conftest import (
    MOCK_DATA_PATH,
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
