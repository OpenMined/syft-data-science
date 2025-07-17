from pathlib import Path
import traceback
from typing import TYPE_CHECKING, Final, Type, Union
from syft_rds.client.exceptions import DatasetExistsError, DatasetNotFoundError
from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models.models import (
    Dataset,
    DatasetCreate,
    DatasetUpdate,
    GetAllRequest,
    GetOneRequest,
)
from syft_core import Client as SyftBoxClient

from .managers import (
    DatasetPathManager,
    DatasetFilesManager,
    DatasetUrlManager,
    DatasetSchemaManager,
)

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
        self._path_manager = DatasetPathManager(self.syftbox_client)
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
            DatasetExistsError: if the Dataset name already exists
            FileNotFoundError: if the private/mock paths don't exist
            NotADirectoryError: if the provided private/mock paths are not directories
        """
        self._validate_dataset_name_unique(name)
        self._validate_path_existence(path, mock_path)
        self._validate_file_extensions(path, mock_path)

    def _validate_dataset_name_unique(self, name: str) -> None:
        """Validate that dataset name is unique."""
        if (
            self._path_manager.get_local_public_dataset_dir(name).exists()
            or self._path_manager.get_local_private_dataset_dir(name).exists()
        ):
            raise DatasetExistsError(f"Dataset with name '{name}' already exists")

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
            self._files_manager.copy_dataset_files(dataset_create)
            dataset = self._schema_manager.create(dataset_create)
            return dataset._register_client_id_recursive(self.config.uid)
        except Exception as e:
            self._files_manager.cleanup_dataset_files(dataset_create.name)
            self._schema_manager.delete(dataset_create.name)
            raise RuntimeError(
                f"Failed to create dataset '{dataset_create.name}': {str(e)}"
            ) from e

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
                raise DatasetNotFoundError(
                    f"Dataset with uid {update_item.uid} not found"
                )

            updated_dataset = existing_dataset.apply_update(update_item, in_place=False)

            if update_item.name:
                self._validate_dataset_name_unique(update_item.name)
                syftbox_client_email = self._path_manager.syftbox_client_email
                old_dataset_name = existing_dataset.name
                new_dataset_name = update_item.name

                self._files_manager.move_dataset_files(
                    old_dataset_name, new_dataset_name
                )

                new_private_url = DatasetUrlManager.get_private_dataset_syftbox_url(
                    syftbox_client_email, updated_dataset.name
                )
                new_mock_url = DatasetUrlManager.get_mock_dataset_syftbox_url(
                    syftbox_client_email, updated_dataset.name
                )
                new_readme_url = DatasetUrlManager.update_readme_syftbox_url(
                    updated_dataset.readme, dataset_name=updated_dataset.name
                )

                updated_dataset.private = new_private_url
                updated_dataset.mock = new_mock_url
                updated_dataset.readme = new_readme_url

            updated_dataset = self.store.update(updated_dataset.uid, updated_dataset)
            return updated_dataset._register_client_id_recursive(self.config.uid)
        except DatasetExistsError:
            raise
        except Exception as e:
            print(traceback.format_exc())
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
