from pathlib import Path
from typing import Union
from syft_core import Client as SyftBoxClient

from ..constants import DIRECTORY_DATASETS, DIRECTORY_PRIVATE, DIRECTORY_PUBLIC


class DatasetPathManager:
    """Manages filesystem paths for dataset operations."""

    def __init__(self, syftbox_client: SyftBoxClient):
        """
        Initialize the path manager.

        Args:
            syftbox_client: The SyftBox client
            host: The host identifier
        """
        self.syftbox_client = syftbox_client

    def get_local_public_dataset_dir(self, dataset_name: str) -> Path:
        """Get the local public directory path for a dataset."""
        return (
            self.syftbox_client.my_datasite
            / DIRECTORY_PUBLIC
            / DIRECTORY_DATASETS
            / dataset_name
        )

    def get_local_private_dataset_dir(self, dataset_name: str) -> Path:
        """Get the local private directory path for a dataset."""
        return (
            self.syftbox_client.my_datasite
            / DIRECTORY_PRIVATE
            / DIRECTORY_DATASETS
            / dataset_name
        )

    def get_local_public_datasets_dir(self) -> Path:
        """Get the local base public directory for all datasets."""
        return self.syftbox_client.my_datasite / DIRECTORY_PUBLIC / DIRECTORY_DATASETS

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
            raise FileNotFoundError(f"Path does not exist: {path}")

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
            raise NotADirectoryError(
                f"Mock and private data paths must be directories: {path} and {mock_path}"
            )
