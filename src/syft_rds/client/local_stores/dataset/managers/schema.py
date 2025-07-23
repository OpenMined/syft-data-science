from pathlib import Path

from syft_core import SyftBoxURL
from syft_rds.models.models import Dataset, DatasetCreate
from syft_rds.store.store import YAMLStore

from .path import DatasetPathManager
from .url import DatasetUrlManager


class DatasetSchemaManager:
    """Manages schema operations for datasets."""

    def __init__(self, path_manager: DatasetPathManager, store: YAMLStore) -> None:
        """
        Initialize the schema manager.

        Args:
            path_manager: The path manager to use
            store: The RDS store for persistence
        """
        self._path_manager = path_manager
        self._schema_store = store

    def create(self, dataset_create: DatasetCreate) -> Dataset:
        """
        Create a dataset schema.

        Args:
            dataset_create: Dataset creation data

        Returns:
            The created dataset
        """
        syftbox_client_email = self._path_manager.syftbox_client_email

        # Generate URLs for the dataset components
        mock_url = DatasetUrlManager.get_mock_dataset_syftbox_url(
            syftbox_client_email, dataset_create.name, Path(dataset_create.mock_path)
        )
        private_url = DatasetUrlManager.get_private_dataset_syftbox_url(
            syftbox_client_email, dataset_create.name, Path(dataset_create.path)
        )

        readme_url: SyftBoxURL = (
            DatasetUrlManager.get_readme_syftbox_url(
                syftbox_client_email,
                dataset_create.name,
                Path(dataset_create.description_path),
            )
            if dataset_create.description_path
            else None
        )

        # Create the dataset schema object
        # TODO: Could we not unpack DatasetCreate directly?
        dataset = Dataset(
            name=dataset_create.name,
            private=private_url,
            mock=mock_url,
            tags=dataset_create.tags,
            summary=dataset_create.summary,
            readme=readme_url,
            auto_approval=dataset_create.auto_approval,
        )
        if dataset_create.runtime:
            dataset.runtime = dataset_create.runtime

        # Persist the schema to store
        self._schema_store.create(dataset)
        return dataset

    def delete(self, name: str) -> bool:
        """
        Delete a dataset by name.

        Args:
            name: Name of the dataset to delete

        Returns:
            True if deleted, False if not found
        """
        queried_result: list[Dataset] = self._schema_store.get_all(
            filters={"name": name}
        )
        if not queried_result:
            return False
        first_res: Dataset = queried_result[0]
        return self._schema_store.delete(first_res.uid)
