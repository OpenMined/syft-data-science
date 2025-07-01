import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Final, Type, Union

from loguru import logger
from syft_core import Client as SyftBoxClient
from syft_core.url import SyftBoxURL

from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models import (
    Dataset,
    DatasetCreate,
    DatasetUpdate,
    GetAllRequest,
    GetOneRequest,
)
from syft_rds.store import YAMLStore

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig

# Constants for path segments to avoid magic strings
DIRECTORY_PUBLIC = "public"
DIRECTORY_PRIVATE = "private"
DIRECTORY_DATASETS = "datasets"


class DatasetPathManager:
    """Manages filesystem paths for dataset operations."""

    def __init__(self, syftbox_client: SyftBoxClient, host: str):
        """
        Initialize the path manager.

        Args:
            syftbox_client: The SyftBox client
            host: The host identifier
        """
        self.syftbox_client = syftbox_client
        self._host = host

    def get_local_public_dataset_dir(self, dataset_name: str) -> Path:
        """Get the local public directory path for a dataset."""
        return (
            self.syftbox_client.my_datasite
            / DIRECTORY_PUBLIC
            / DIRECTORY_DATASETS
            / dataset_name
        )

    def get_remote_public_dataset_dir(self, dataset_name: str) -> Path:
        """Get the remote public directory path for a dataset."""
        return (
            self.syftbox_client.datasites
            / self._host
            / DIRECTORY_PUBLIC
            / DIRECTORY_DATASETS
            / dataset_name
        )

    def get_syftbox_private_dataset_dir(self, dataset_name: str) -> Path:
        """Get the private directory path for a dataset."""
        return (
            self.syftbox_client.datasites
            / self._host
            / DIRECTORY_PRIVATE
            / DIRECTORY_DATASETS
            / dataset_name
        )

    def get_remote_public_datasets_dir(self) -> Path:
        """Get the base public directory for all datasets."""
        return (
            self.syftbox_client.datasites
            / self._host
            / DIRECTORY_PUBLIC
            / DIRECTORY_DATASETS
        )

    @property
    def syftbox_client_email(self) -> str:
        """Get the email of the SyftBox client."""
        return self.syftbox_client.email

    def validate_path_exists(self, path: Union[str, Path]) -> None:
        """
        Validate that a path exists.

        Args:
            path: The path to validate

        Raises:
            ValueError: If the path doesn't exist
        """
        if not Path(path).exists():
            raise ValueError(f"Path does not exist: {path}")

    def validate_directory_paths(
        self, path: Union[str, Path], mock_path: Union[str, Path]
    ) -> None:
        """
        Validate that both paths are directories.

        Args:
            path: The first path to validate
            mock_path: The second path to validate

        Raises:
            ValueError: If either path is not a directory
        """
        path, mock_path = Path(path), Path(mock_path)
        if not (path.is_dir() and mock_path.is_dir()):
            raise ValueError(
                f"Mock and private data paths must be directories: {path} and {mock_path}"
            )


class DatasetUrlManager:
    """Manages SyftBox URLs for datasets."""

    @staticmethod
    def get_mock_dataset_syftbox_url(
        datasite_email: str, dataset_name: str, mock_path: Union[Path, str]
    ) -> SyftBoxURL:
        """Generate a SyftBox URL for the mock dataset."""
        return SyftBoxURL(
            f"syft://{datasite_email}/{DIRECTORY_PUBLIC}/{DIRECTORY_DATASETS}/{dataset_name}"
        )

    @staticmethod
    def get_private_dataset_syftbox_url(
        datasite_email: str, dataset_name: str, path: Union[Path, str]
    ) -> SyftBoxURL:
        """Generate a SyftBox URL for the private dataset."""
        return SyftBoxURL(
            f"syft://{datasite_email}/{DIRECTORY_PRIVATE}/{DIRECTORY_DATASETS}/{dataset_name}"
        )

    @staticmethod
    def get_readme_syftbox_url(
        datasite_email: str, dataset_name: str, readme_path: Union[Path, str]
    ) -> SyftBoxURL:
        """Generate a SyftBox URL for the readme file."""
        return SyftBoxURL(
            f"syft://{datasite_email}/{DIRECTORY_PUBLIC}/{DIRECTORY_DATASETS}/{dataset_name}/{Path(readme_path).name}"
        )


class DatasetFilesManager:
    """Manages file operations for datasets."""

    def __init__(self, path_manager: DatasetPathManager) -> None:
        """
        Initialize the files manager.

        Args:
            path_manager: The path manager to use
        """
        self._path_manager = path_manager

    def validate_file_extensions(
        self, path: Union[str, Path], mock_path: Union[str, Path]
    ) -> None:
        """
        Validate that both directories contain the same file extensions.

        Args:
            path: Path to the original data directory
            mock_path: Path to the mock data directory

        Raises:
            ValueError: If the directories contain different file extensions
        """
        path = Path(path)
        mock_path = Path(mock_path)

        if not (path.is_dir() and mock_path.is_dir()):
            raise ValueError("Both paths must be directories")

        # Get all file extensions from the first directory into a set
        path_extensions = self._collect_file_extensions(path)

        # Get all file extensions from the second directory into a set
        mock_extensions = self._collect_file_extensions(mock_path)

        # Compare the sets of extensions
        if path_extensions != mock_extensions:
            self._report_extension_differences(
                path, mock_path, path_extensions, mock_extensions
            )

    def _collect_file_extensions(self, directory: Path) -> set:
        """Collect all file extensions from a directory."""
        extensions = set()
        for file_path in directory.glob("**/*"):
            if file_path.is_file() and file_path.suffix:
                extensions.add(file_path.suffix.lower())
        return extensions

    def _report_extension_differences(
        self, path: Path, mock_path: Path, path_extensions: set, mock_extensions: set
    ) -> None:
        """Report differences in file extensions between directories."""
        extra_in_path = path_extensions - mock_extensions
        extra_in_mock = mock_extensions - path_extensions

        error_msg = "Directories contain different file extensions:\n"
        if extra_in_path:
            error_msg += f"Extensions in {path} but not in {mock_path}: {', '.join(extra_in_path)}\n"
        if extra_in_mock:
            error_msg += f"Extensions in {mock_path} but not in {path}: {', '.join(extra_in_mock)}"

        raise ValueError(error_msg)

    def copy_directory(self, src: Union[str, Path], dest_dir: Path) -> Path:
        """
        Copy a directory to a destination path.

        Args:
            src: Source directory
            dest_dir: Destination directory

        Returns:
            Path to the destination directory

        Raises:
            ValueError: If the source is not a directory
        """
        src_path = Path(src)
        dest_dir.mkdir(parents=True, exist_ok=True)

        if not src_path.is_dir():
            raise ValueError(f"Source path is not a directory: {src_path}")

        # Iterate through all items in the source directory
        for item in src_path.iterdir():
            item_dest = dest_dir / item.name

            if item.is_dir():
                # Recursively copy subdirectories
                shutil.copytree(item, item_dest, dirs_exist_ok=True)
            else:
                # Copy files
                shutil.copy2(item, dest_dir)
        return dest_dir

    def copy_mock_to_public_syftbox_dir(
        self, dataset_name: str, mock_path: Union[str, Path]
    ) -> Path:
        """Copy mock data to the public SyftBox directory."""
        public_dataset_dir: Path = self._path_manager.get_local_public_dataset_dir(
            dataset_name
        )
        return self.copy_directory(mock_path, public_dataset_dir)

    def copy_private_to_private_syftbox_dir(
        self,
        dataset_name: str,
        path: Union[str, Path],
    ) -> Path:
        """Copy private data to the private SyftBox directory."""
        private_dataset_dir: Path = self._path_manager.get_syftbox_private_dataset_dir(
            dataset_name
        )
        return self.copy_directory(path, private_dataset_dir)

    def copy_description_file_to_public_syftbox_dir(
        self, dataset_name: str, description_path: Union[str, Path]
    ) -> Path:
        """Copy description file to the public SyftBox directory."""
        public_dataset_dir: Path = self._path_manager.get_local_public_dataset_dir(
            dataset_name
        )
        if not Path(description_path).exists():
            raise ValueError(f"Description file does not exist: {description_path}")
        dest_path = public_dataset_dir / Path(description_path).name
        shutil.copy2(description_path, dest_path)
        return dest_path

    def cleanup_dataset_files(self, name: str) -> None:
        """
        Remove all dataset files for a given dataset name.

        Args:
            name: Name of the dataset to clean up

        Raises:
            RuntimeError: If cleanup fails
        """
        try:
            public_dir = self._path_manager.get_local_public_dataset_dir(name)
            private_dir = self._path_manager.get_syftbox_private_dataset_dir(name)

            self._safe_remove_directory(public_dir)
            self._safe_remove_directory(private_dir)
        except Exception as e:
            logger.error(f"Failed to cleanup dataset files: {str(e)}")
            raise RuntimeError(f"Failed to clean up dataset '{name}'") from e

    def _safe_remove_directory(self, directory: Path) -> None:
        """Safely remove a directory if it exists."""
        if directory.exists():
            shutil.rmtree(directory)


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
        dataset = Dataset(
            name=dataset_create.name,
            private=private_url,
            mock=mock_url,
            tags=dataset_create.tags,
            summary=dataset_create.summary,
            readme=readme_url,
        )
        if dataset_create.runtime_id:
            dataset.runtime_id = dataset_create.runtime_id

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

    def update(self, item: DatasetUpdate) -> Dataset:
        """Not implemented for Dataset."""
        raise NotImplementedError("Not implemented for Dataset")

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
