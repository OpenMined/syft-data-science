from pathlib import Path
import pandas as pd

from syft_rds.client.rds_client import RDSClient, init_session
from syft_core import SyftClientConfig


TEST_DIR = Path(__file__).parent


def test_create_dataset(do_syftbox_config: SyftClientConfig) -> None:
    do_rds_client: RDSClient = init_session(
        host=do_syftbox_config.email, syftbox_client_config_path=do_syftbox_config.path
    )
    assert do_rds_client.is_admin

    private_data_path = TEST_DIR / "assets/do/data.csv"
    mock_data_path = TEST_DIR / "assets/do/mock.csv"
    data = do_rds_client.dataset.create(
        name="Test",
        path=private_data_path,
        mock_path=mock_data_path,
        file_type="csv",
        summary="Test data",
        description_path=TEST_DIR / "assets/do/README.md",
    )

    assert data.name == "Test"
    assert data.get_mock_path().exists()
    assert data.get_private_path().exists()
    assert data.summary == "Test data"
    assert data.file_type == "csv"
    private_df = pd.read_csv(data.get_private_path())
    assert private_df.equals(pd.read_csv(private_data_path))
    mock_df = pd.read_csv(data.get_mock_path())
    assert mock_df.equals(pd.read_csv(mock_data_path))
