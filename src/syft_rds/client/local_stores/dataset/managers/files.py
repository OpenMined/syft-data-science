import shutil
from pathlib import Path
from typing import Union
from loguru import logger

from syft_rds.models.models import DatasetCreate

from .path import DatasetPathManager


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
        private_dataset_dir: Path = self._path_manager.get_local_private_dataset_dir(
            dataset_name
        )
        return self.copy_directory(path, private_dataset_dir)

    def copy_description_file_to_public_syftbox_dir(
        self, dataset_name: str, description_path: Union[str, Path, None]
    ) -> Path | None:
        """Copy description file to the public SyftBox directory."""
        if not description_path:
            return

        public_dataset_dir: Path = self._path_manager.get_local_public_dataset_dir(
            dataset_name
        )
        if not Path(description_path).exists():
            raise ValueError(f"Description file does not exist: {description_path}")
        dest_path = public_dataset_dir / Path(description_path).name
        shutil.copy2(description_path, dest_path)
        return dest_path

    def copy_dataset_files(self, dataset_create: DatasetCreate) -> None:
        """Copy all necessary files for a new dataset."""
        self.copy_mock_to_public_syftbox_dir(
            dataset_create.name, dataset_create.mock_path
        )
        self.copy_description_file_to_public_syftbox_dir(
            dataset_create.name, dataset_create.description_path
        )
        self.copy_private_to_private_syftbox_dir(
            dataset_create.name, dataset_create.path
        )

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
            private_dir = self._path_manager.get_local_private_dataset_dir(name)

            self._safe_remove_directory(public_dir)
            self._safe_remove_directory(private_dir)
        except Exception as e:
            logger.error(f"Failed to cleanup dataset files: {str(e)}")
            raise RuntimeError(f"Failed to clean up dataset '{name}'") from e

    def move_dataset_files(
        self, src_dataset_name: str, dst_dataset_name: str, overwrite: bool = False
    ) -> None:
        src_public_dir = self._path_manager.get_local_public_dataset_dir(
            src_dataset_name
        )
        src_private_dir: Path = self._path_manager.get_local_private_dataset_dir(
            src_dataset_name
        )

        if not src_public_dir.exists() or not src_private_dir.exists():
            raise FileNotFoundError("Source directories don't exist.")

        dst_public_dir: Path = self._path_manager.get_local_public_dataset_dir(
            dst_dataset_name
        )
        dst_private_dir: Path = self._path_manager.get_local_private_dataset_dir(
            dst_dataset_name
        )

        if not overwrite and (dst_public_dir.exists() or dst_private_dir.exists()):
            raise FileExistsError("Destination directories already exist.")

        if dst_public_dir.exists():
            shutil.rmtree(dst_public_dir)
        if dst_private_dir.exists():
            shutil.rmtree(dst_private_dir)

        shutil.move(src_public_dir, dst_public_dir)
        shutil.move(src_private_dir, dst_private_dir)

    def _safe_remove_directory(self, directory: Path) -> None:
        """Safely remove a directory if it exists."""
        if directory.exists():
            shutil.rmtree(directory)
