from conftest import MOCK_DATA_PATH, PRIVATE_DATA_PATH, README_PATH


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
