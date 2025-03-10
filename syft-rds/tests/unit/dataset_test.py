import pandas as pd
import pytest

from syft_rds.client.rds_client import RDSClient, init_session
from syft_core import SyftClientConfig

from tests.conftest import MOCK_DATA_PATH, PRIVATE_DATA_PATH, README_PATH
from tests.utils import create_dataset


def test_create_dataset(do_syftbox_config: SyftClientConfig) -> None:
    do_rds_client: RDSClient = init_session(
        host=do_syftbox_config.email, syftbox_client_config_path=do_syftbox_config.path
    )
    assert do_rds_client.is_admin

    dataset = create_dataset(do_rds_client, "Test")
    assert dataset.name == "Test"
    assert dataset.get_mock_path().exists()
    assert dataset.get_private_path().exists()
    assert dataset.summary == "Test data"
    assert dataset.describe()
    private_df = pd.read_csv(dataset.get_private_path() / "data.csv")
    assert private_df.equals(pd.read_csv(PRIVATE_DATA_PATH / "data.csv"))
    mock_df = pd.read_csv(dataset.get_mock_path() / "data.csv")
    assert mock_df.equals(pd.read_csv(MOCK_DATA_PATH / "data.csv"))


def test_get_dataset(do_syftbox_config: SyftClientConfig) -> None:
    do_rds_client: RDSClient = init_session(
        host=do_syftbox_config.email, syftbox_client_config_path=do_syftbox_config.path
    )
    assert do_rds_client.is_admin

    dataset = create_dataset(do_rds_client, "Test")
    test_data = do_rds_client.dataset.get(name="Test")
    assert test_data.name == dataset.name
    assert test_data.get_mock_path().exists()
    assert test_data.get_private_path().exists()
    assert test_data.summary == dataset.summary
    private_df = pd.read_csv(test_data.get_private_path() / "data.csv")
    assert private_df.equals(pd.read_csv(PRIVATE_DATA_PATH / "data.csv"))
    mock_df = pd.read_csv(test_data.get_mock_path() / "data.csv")
    assert mock_df.equals(pd.read_csv(MOCK_DATA_PATH / "data.csv"))


def test_get_all_datasets(do_syftbox_config: SyftClientConfig) -> None:
    do_rds_client: RDSClient = init_session(
        host=do_syftbox_config.email, syftbox_client_config_path=do_syftbox_config.path
    )
    assert do_rds_client.is_admin

    dataset_1 = create_dataset(do_rds_client, "Test")
    datasets = do_rds_client.datasets
    assert len(datasets) == 1

    dataset_2 = create_dataset(do_rds_client, "Test 2")
    datasets = do_rds_client.dataset.get_all()
    assert len(datasets) == 2

    assert dataset_1 in datasets
    assert dataset_2 in datasets


def test_delete_dataset(do_syftbox_config: SyftClientConfig) -> None:
    """Test deleting a dataset and verifying it's removed from storage and filesystem."""
    do_rds_client: RDSClient = init_session(
        host=do_syftbox_config.email, syftbox_client_config_path=do_syftbox_config.path
    )
    assert do_rds_client.is_admin

    # Create a dataset to delete
    dataset = create_dataset(do_rds_client, "TestToDelete")

    # Verify it exists
    mock_path = dataset.get_mock_path()
    private_path = dataset.get_private_path()
    assert mock_path.exists()
    assert private_path.exists()

    # Get all datasets before deletion
    datasets_before = do_rds_client.dataset.get_all()
    assert any(d.name == "TestToDelete" for d in datasets_before)

    # Delete the dataset
    success = do_rds_client.dataset.delete("TestToDelete")
    assert success is True

    # Verify it's gone from the list
    datasets_after = do_rds_client.dataset.get_all()
    assert not any(d.name == "TestToDelete" for d in datasets_after)
    assert len(datasets_after) == len(datasets_before) - 1

    # Try to get the deleted dataset (should raise an error)
    with pytest.raises(ValueError, match="No Dataset found with"):
        do_rds_client.dataset.get(name="TestToDelete")

    # Verify files are cleaned up (should no longer exist)
    assert not mock_path.parent.exists() or not any(mock_path.parent.iterdir())
    assert not private_path.parent.exists() or not any(private_path.parent.iterdir())


def test_delete_nonexistent_dataset(do_syftbox_config: SyftClientConfig) -> None:
    """Test deleting a dataset that doesn't exist returns False."""
    do_rds_client: RDSClient = init_session(
        host=do_syftbox_config.email, syftbox_client_config_path=do_syftbox_config.path
    )
    assert do_rds_client.is_admin

    # Try to delete a non-existent dataset
    success = do_rds_client.dataset.delete("NonExistentDataset")
    assert success is False


def test_permission_error_non_admin(
    do_syftbox_config: SyftClientConfig, ds_syftbox_config: SyftClientConfig
) -> None:
    """Test that non-admin users cannot create or delete datasets."""
    do_rds_client: RDSClient = init_session(
        host=do_syftbox_config.email, syftbox_client_config_path=do_syftbox_config.path
    )
    assert do_rds_client.is_admin
    ds_rds_client: RDSClient = init_session(
        host=do_syftbox_config.email, syftbox_client_config_path=ds_syftbox_config.path
    )
    assert not ds_rds_client.is_admin

    # Attempt to create a dataset as non-admin
    with pytest.raises(PermissionError, match="You must be the datasite admin"):
        create_dataset(ds_rds_client, "TestPermissionError")

    # Create a dataset as admin first
    dataset = create_dataset(do_rds_client, "TestPermissionError")

    # Then try to delete it as non-admin
    with pytest.raises(PermissionError, match="You must be the datasite admin"):
        ds_rds_client.dataset.delete(dataset.name)


def test_create_datasets_same_name(do_syftbox_config: SyftClientConfig) -> None:
    """Test that creating a dataset with an existing name raises an error."""
    do_rds_client: RDSClient = init_session(
        host=do_syftbox_config.email, syftbox_client_config_path=do_syftbox_config.path
    )
    assert do_rds_client.is_admin

    # Create a dataset
    create_dataset(do_rds_client, "DuplicateName")

    with pytest.raises(
        ValueError, match="Dataset with name 'DuplicateName' already exists"
    ):
        create_dataset(do_rds_client, "DuplicateName")


def test_readme_content(do_syftbox_config: SyftClientConfig) -> None:
    """Test that README content is correctly stored and retrieved."""
    do_rds_client: RDSClient = init_session(
        host=do_syftbox_config.email, syftbox_client_config_path=do_syftbox_config.path
    )
    assert do_rds_client.is_admin

    # Create a dataset with a README
    dataset = create_dataset(do_rds_client, "TestReadme")

    # Retrieve the dataset and check README content
    retrieved = do_rds_client.dataset.get(name=dataset.name)

    # Assuming we've implemented the get_description method
    readme_content = retrieved.get_description()
    assert readme_content is not None
    assert len(readme_content) > 0

    # Compare with the original file
    with open(README_PATH, "r") as f:
        original_content = f.read()

    assert readme_content == original_content
