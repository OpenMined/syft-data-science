from pathlib import Path
from typing import TYPE_CHECKING, Final, Type, Union
from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models.models import (
    Dataset,
    DatasetCreate,
    DatasetUpdate,
    GetAllRequest,
    GetOneRequest,
)
from syft_core import Client as SyftBoxClient


from .managers.files import DatasetFilesManager
from .managers.path import DatasetPathManager
from .managers.url import DatasetUrlManager
from .managers.schema import DatasetSchemaManager

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig


class DatasetLocalStore(CRUDLocalStore[Dataset, DatasetCreate, DatasetUpdate]):
    """Local store for dataset operations."""

    ITEM_TYPE: Final[Type[Dataset]] = Dataset

    def __init__(self, config: "RDSClientConfig", syftbox_client: SyftBoxClient):
        """
        Initialize the dataset local store.

        Args:
            config: The RDS client configuration
            syftbox_client: The SyftBox client
        """
        super().__init__(config, syftbox_client)
        self._path_manager = DatasetPathManager(self.syftbox_client, self.config.host)
        self._files_manager = DatasetFilesManager(self._path_manager)
        self._schema_manager = DatasetSchemaManager(self._path_manager, self.store)

    def _validate_dataset_paths(
        self,
        name: str,
        path: Union[str, Path],
        mock_path: Union[str, Path],
    ) -> None:
        """
        Validate all aspects of dataset paths.

        Args:
            name: Dataset name
            path: Path to private data
            mock_path: Path to mock data

        Raises:
            ValueError: If validation fails
        """
        self._validate_dataset_name_unique(name)
        self._validate_path_existence(path, mock_path)
        self._validate_file_extensions(path, mock_path)

    def _validate_dataset_name_unique(self, name: str) -> None:
        """Validate that dataset name is unique."""
        if (
            self._path_manager.get_local_public_dataset_dir(name).exists()
            or self._path_manager.get_syftbox_private_dataset_dir(name).exists()
        ):
            raise ValueError(f"Dataset with name '{name}' already exists")

    def _validate_path_existence(
        self, path: Union[str, Path], mock_path: Union[str, Path]
    ) -> None:
        """Validate that paths exist and are directories."""
        self._path_manager.validate_path_exists(path)
        self._path_manager.validate_path_exists(mock_path)
        self._path_manager.validate_directory_paths(path, mock_path)

    def _validate_file_extensions(
        self, path: Union[str, Path], mock_path: Union[str, Path]
    ) -> None:
        """Validate that file extensions match between directories."""
        self._files_manager.validate_file_extensions(path, mock_path)

    def create(self, dataset_create: DatasetCreate) -> Dataset:
        """
        Create a new dataset.

        Args:
            dataset_create: Dataset creation data

        Returns:
            The created dataset

        Raises:
            RuntimeError: If creation fails
        """
        self._validate_dataset_paths(
            dataset_create.name,
            dataset_create.path,
            dataset_create.mock_path,
        )
        try:
            self._copy_dataset_files(dataset_create)
            dataset = self._schema_manager.create(dataset_create)
            return dataset._register_client_id_recursive(self.config.uid)
        except Exception as e:
            self._files_manager.cleanup_dataset_files(dataset_create.name)
            self._schema_manager.delete(dataset_create.name)
            raise RuntimeError(
                f"Failed to create dataset '{dataset_create.name}': {str(e)}"
            ) from e

    def _copy_dataset_files(self, dataset_create: DatasetCreate) -> None:
        """Copy all necessary files for a new dataset."""
        self._files_manager.copy_mock_to_public_syftbox_dir(
            dataset_create.name, dataset_create.mock_path
        )
        self._files_manager.copy_description_file_to_public_syftbox_dir(
            dataset_create.name, dataset_create.description_path
        )
        self._files_manager.copy_private_to_private_syftbox_dir(
            dataset_create.name, dataset_create.path
        )

    def get_all(self, request: GetAllRequest) -> list[Dataset]:
        """
        Get all datasets.

        Args:
            request: The get all request object

        Returns:
            List of all datasets
        """
        return super().get_all(request)

    def update(self, update_item: DatasetUpdate) -> Dataset:
        """
        Update an existing dataset.

        Args:
            update_dataset: The dataset update data

        Returns:
            The updated dataset

        Raises:
            RuntimeError: If update fails
        """
        try:
            existing_dataset = self.store.get_by_uid(update_item.uid)
            if existing_dataset is None:
                raise ValueError(f"Dataset with uid {update_item.uid} not found")

            updated_dataset = existing_dataset.apply_update(update_item)

            if update_item.name:
                syftbox_client_email = self._path_manager.syftbox_client_email
                new_private_url = DatasetUrlManager.get_private_dataset_syftbox_url(
                    syftbox_client_email, update_item.name
                )
                update_item = new_private_url

            return self.store.update(updated_dataset.uid, updated_dataset)
        except Exception as e:
            raise RuntimeError(
                f"Failed to update dataset with uid {update_item.uid}: {str(e)}"
            )

    def get(self, request: GetOneRequest) -> Dataset:
        """
        Get a dataset based on name / id

        Args:
            request: The get one request object

        Returns:
            The dataset
        """
        return super().get_one(request)

    def delete_by_name(self, name: str) -> bool:
        """
        Delete a dataset by name.

        Args:
            name: Name of the dataset to delete

        Returns:
            True if deleted, False if not found

        Raises:
            RuntimeError: If deletion fails
        """
        try:
            schema_deleted = self._schema_manager.delete(name)
            if schema_deleted:
                self._files_manager.cleanup_dataset_files(name)
                return True
            return False
        except Exception as e:
            raise RuntimeError(f"Failed to delete dataset '{name}'") from e
