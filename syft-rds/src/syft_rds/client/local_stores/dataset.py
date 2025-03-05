from typing import Final, Type


from pathlib import Path
from typing import TYPE_CHECKING, Union
import shutil
from loguru import logger

from syft_core.url import SyftBoxURL
from syft_core import Client as SyftBoxClient

from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models.models import (
    Dataset,
    DatasetCreate,
    DatasetUpdate,
    GetOneRequest,
    GetAllRequest,
)
from syft_rds.store import RDSStore

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig


class DatasetLocalStore(CRUDLocalStore[Dataset, DatasetCreate, DatasetUpdate]):
    SCHEMA: Final[Type[Dataset]] = Dataset

    def __init__(self, config: "RDSClientConfig", syftbox_client: SyftBoxClient):
        super().__init__(config, syftbox_client)
        self._path_manager = DatasetPathManager(self.syftbox_client, self.config.host)
        self._schema_manager = DatasetSchemaManager(self._path_manager, self.store)
        self._files_manager = DatasetFilesManager(self._path_manager)

    def _validate_paths(
        self,
        name: str,
        path: Union[str, Path],
        mock_path: Union[str, Path],
    ) -> None:
        if (
            self._path_manager.get_local_public_dataset_dir(name).exists()
            or self._path_manager.get_syftbox_private_dataset_dir(name).exists()
        ):
            raise ValueError(f"Dataset with name '{name}' already exists")
        # validate paths
        self._path_manager.check_path_exists(path)
        self._path_manager.check_path_exists(mock_path)
        self._path_manager.check_are_both_dirs(path, mock_path)
        # validate dataset file extensions
        self._files_manager.check_same_file_extensions(path, mock_path)

    def create(self, dataset_create: DatasetCreate) -> Dataset:
        self._validate_paths(
            dataset_create.name,
            dataset_create.path,
            dataset_create.mock_path,
        )
        try:
            self._files_manager.copy_mock_to_public_syftbox_dir(
                dataset_create.name, dataset_create.mock_path
            )
            self._files_manager.copy_description_file_to_public_syftbox_dir(
                dataset_create.name, dataset_create.description_path
            )
            self._files_manager.copy_private_to_private_syftbox_dir(
                dataset_create.name, dataset_create.path
            )
            dataset = self._schema_manager.create(dataset_create)
            return dataset.with_client(self.syftbox_client)
        except Exception as e:
            self._files_manager.cleanup_dataset_files(dataset_create.name)
            raise RuntimeError(
                f"Failed to create dataset '{dataset_create.name}': {str(e)}"
            ) from e

    def get_one(self, request: GetOneRequest) -> Dataset:
        raise NotImplementedError("Not implemented for Dataset")

    def get_all(self, request: GetAllRequest) -> list[Dataset]:
        datasets: list[Dataset] = self._schema_manager.get_all()
        return [dataset.with_client(self.syftbox_client) for dataset in datasets]

    def update(self, item: DatasetUpdate) -> Dataset:
        raise NotImplementedError("Not implemented for Dataset")

    def get_by_name(self, name: str) -> Dataset:
        dataset: Dataset = self._schema_manager.get_by_name(name=name)
        return dataset.with_client(self.syftbox_client)

    def delete_by_name(self, name: str) -> bool:
        try:
            schema_deleted = self._schema_manager.delete(name)
            if schema_deleted:
                self._files_manager.cleanup_dataset_files(name)
                return True
            return False
        except Exception as e:
            raise RuntimeError(f"Failed to delete dataset '{name}'") from e


class DatasetUrlManager:
    @staticmethod
    def get_mock_dataset_syftbox_url(
        datasite_email: str, dataset_name: str, mock_path: Union[Path, str]
    ) -> SyftBoxURL:
        return SyftBoxURL(f"syft://{datasite_email}/public/datasets/{dataset_name}")

    @staticmethod
    def get_private_dataset_syftbox_url(
        datasite_email: str, dataset_name: str, path: Union[Path, str]
    ) -> str:
        return f"syft://{datasite_email}/private/datasets/{dataset_name}"

    @staticmethod
    def get_readme_syftbox_url(
        datasite_email: str, dataset_name: str, readme_path: Union[Path, str]
    ) -> SyftBoxURL:
        return SyftBoxURL(
            f"syft://{datasite_email}/public/datasets/{dataset_name}/{Path(readme_path).name}"
        )


class DatasetPathManager:
    def __init__(self, syftbox_client: SyftBoxClient, host: str):
        self.syftbox_client = syftbox_client
        self._host = host

    def get_local_public_dataset_dir(self, dataset_name: str) -> Path:
        return self.syftbox_client.my_datasite / "public" / "datasets" / dataset_name

    def get_remote_public_dataset_dir(self, dataset_name: str) -> Path:
        return (
            self.syftbox_client.datasites
            / self._host
            / "public"
            / "datasets"
            / dataset_name
        )

    def get_syftbox_private_dataset_dir(self, dataset_name: str) -> Path:
        return (
            self.syftbox_client.datasites
            / self._host
            / "private"
            / "datasets"
            / dataset_name
        )

    def get_remote_public_datasets_dir(self) -> Path:
        return self.syftbox_client.datasites / self._host / "public" / "datasets"

    @property
    def syftbox_client_email(self) -> str:
        return self.syftbox_client.email

    def check_path_exists(self, path: Union[str, Path]):
        if not Path(path).exists():
            raise ValueError(f"Path does not exist: {path}")

    def check_are_both_dirs(self, path: Union[str, Path], mock_path: Union[str, Path]):
        path, mock_path = Path(path), Path(mock_path)
        if not (path.is_dir() and mock_path.is_dir()):
            raise ValueError(
                f"Mock and private data paths must be directories: {path} and {mock_path}"
            )


class DatasetFilesManager:
    def __init__(self, path_manager: DatasetPathManager) -> None:
        self._path_manager = path_manager

    def check_same_file_extensions(
        self, path: Union[str, Path], mock_path: Union[str, Path]
    ) -> bool:
        path = Path(path)
        mock_path = Path(mock_path)
        if path.is_dir() and mock_path.is_dir():
            # Get all file extensions from the first directory into a set
            path_extensions = set()
            for file_path in path.glob("**/*"):
                if file_path.is_file() and file_path.suffix:
                    path_extensions.add(file_path.suffix.lower())

            # Get all file extensions from the second directory into a set
            mock_extensions = set()
            for file_path in mock_path.glob("**/*"):
                if file_path.is_file() and file_path.suffix:
                    mock_extensions.add(file_path.suffix.lower())

            # Compare the sets of extensions
            if path_extensions != mock_extensions:
                extra_in_path = path_extensions - mock_extensions
                extra_in_mock = mock_extensions - path_extensions
                error_msg = "Directories contain different file extensions:\n"
                if extra_in_path:
                    error_msg += f"Extensions in {path} but not in {mock_path}: {', '.join(extra_in_path)}\n"
                if extra_in_mock:
                    error_msg += f"Extensions in {mock_path} but not in {path}: {', '.join(extra_in_mock)}"
                raise ValueError(error_msg)

            return True

        return False

    @staticmethod
    def _copy_dir(src: Union[str, Path], dest_dir: Path) -> Path:
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
        public_dataset_dir: Path = self._path_manager.get_local_public_dataset_dir(
            dataset_name
        )
        return self._copy_dir(mock_path, public_dataset_dir)

    def copy_private_to_private_syftbox_dir(
        self,
        dataset_name: str,
        path: Union[str, Path],
    ) -> Path:
        private_dataset_dir: Path = self._path_manager.get_syftbox_private_dataset_dir(
            dataset_name
        )
        return self._copy_dir(path, private_dataset_dir)

    def copy_description_file_to_public_syftbox_dir(
        self, dataset_name: str, description_path: Union[str, Path]
    ) -> Path:
        public_dataset_dir: Path = self._path_manager.get_local_public_dataset_dir(
            dataset_name
        )
        dest_path = public_dataset_dir / Path(description_path).name
        shutil.copy2(description_path, dest_path)
        return dest_path

    def cleanup_dataset_files(self, name: str) -> None:
        try:
            public_dir = self._path_manager.get_local_public_dataset_dir(name)
            private_dir = self._path_manager.get_syftbox_private_dataset_dir(name)
            shutil.rmtree(public_dir)
            shutil.rmtree(private_dir)
        except Exception as e:
            logger.error(f"Failed to cleanup dataset files: {str(e)}")
            raise RuntimeError(f"Failed to clean up dataset '{name}': {str(e)}") from e


class DatasetSchemaManager:
    def __init__(self, path_manager: DatasetPathManager, store: RDSStore) -> None:
        self._path_manager = path_manager
        self._schema_store = store

    def create(self, dataset_create: DatasetCreate) -> None:
        syftbox_client_email = self._path_manager.syftbox_client_email
        mock_url: SyftBoxURL = DatasetUrlManager.get_mock_dataset_syftbox_url(
            syftbox_client_email, dataset_create.name, Path(dataset_create.mock_path)
        )
        private_url: SyftBoxURL = DatasetUrlManager.get_private_dataset_syftbox_url(
            syftbox_client_email, dataset_create.name, Path(dataset_create.path)
        )
        readme_url: SyftBoxURL = DatasetUrlManager.get_readme_syftbox_url(
            syftbox_client_email,
            dataset_create.name,
            Path(dataset_create.description_path),
        )
        dataset = Dataset(
            name=dataset_create.name,
            private=private_url,
            mock=mock_url,
            tags=dataset_create.tags,
            summary=dataset_create.summary,
            readme=readme_url,
        )
        self._schema_store.create(dataset)
        return dataset

    def delete(self, name: str) -> bool:
        queried_result = self._schema_store.query(name=name)
        if not queried_result:
            return False
        return self._schema_store.delete(queried_result[0].uid)

    def get_by_name(self, name: str) -> Dataset:
        queried_result = self._schema_store.query(name=name)
        if not queried_result:
            raise ValueError(f"Dataset with name '{name}' not found")
        if len(queried_result) > 1:
            raise ValueError(f"Multiple datasets found with name '{name}'")
        return queried_result[0]

    def get_all(self) -> list[Dataset]:
        return self._schema_store.list_all()
