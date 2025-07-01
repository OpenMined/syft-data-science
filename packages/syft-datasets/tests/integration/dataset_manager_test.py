import pytest
from conftest import MOCK_DATA_PATH, PRIVATE_DATA_PATH, README_PATH
from syft_datasets.dataset_manager import SyftDatasetManager


def test_create_dataset(dataset_manager):
    dataset = dataset_manager.create(
        name="test_dataset",
        mock_path=MOCK_DATA_PATH,
        private_path=PRIVATE_DATA_PATH,
        summary="A test dataset",
        readme_path=README_PATH,
    )

    assert dataset.mock_dir.exists()
    assert dataset.readme_path.exists()
    assert dataset.private_dir.exists()
    assert dataset.private_config_path.exists()


def test_get_dataset(dataset_manager: SyftDatasetManager):
    do_syftbox_client = dataset_manager.syftbox_client

    dataset_1 = dataset_manager.create(
        name="test_dataset",
        mock_path=MOCK_DATA_PATH,
        private_path=PRIVATE_DATA_PATH,
        summary="A test dataset",
        readme_path=README_PATH,
    )

    dataset_2 = dataset_manager.create(
        name="test_dataset_2",
        mock_path=MOCK_DATA_PATH,
        private_path=PRIVATE_DATA_PATH,
        summary="Another test dataset",
        readme_path=README_PATH,
    )

    retrieved_dataset_1 = dataset_manager.get(
        datasite=do_syftbox_client.email,
        name=dataset_1.name,
    )

    retrieved_without_datasite = dataset_manager.get(
        name=dataset_1.name,
    )

    assert retrieved_dataset_1.name == dataset_1.name
    assert retrieved_without_datasite.name == dataset_1.name

    with pytest.raises(FileNotFoundError):
        _ = dataset_manager.get(
            datasite="wrong_datasite",
            name=dataset_1.name,
        )

    with pytest.raises(FileNotFoundError):
        _ = dataset_manager.get(
            datasite=do_syftbox_client.email,
            name="wrong_dataset_name",
        )

    all_datasets = dataset_manager.get_all()
    assert len(all_datasets) == 2
    assert dataset_1.name in [d.name for d in all_datasets]
    assert dataset_2.name in [d.name for d in all_datasets]

    all_datasets_from_datasite = dataset_manager.get_all(
        datasite=do_syftbox_client.email,
    )
    assert len(all_datasets_from_datasite) == 2

    all_datasets_from_wrong_datasite = dataset_manager.get_all(
        datasite="wrong_datasite",
    )
    assert len(all_datasets_from_wrong_datasite) == 0
