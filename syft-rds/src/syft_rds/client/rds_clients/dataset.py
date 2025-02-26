from pathlib import Path
from typing import Optional, Union
import shutil
from loguru import logger
from functools import wraps

from syft_core import Client as SyftBoxClient
from syft_core.url import SyftBoxURL
from syft_rds.specs import DatasetSpec
from syft_rds.store import RDSStore

from syft_rds.client.rds_clients.base import RDSClientConfig
from syft_rds.models.models import DatasetCreate, Dataset, DatasetUpdate


class DatasetUrlManager:
    @staticmethod
    def get_mock_dataset_syftbox_url(
        datasite_email: str, dataset_name: str, mock_path: Union[Path, str]
    ) -> SyftBoxURL:
        return SyftBoxURL(
            f"syft://{datasite_email}/public/datasets/{dataset_name}/{Path(mock_path).name}"
        )

    @staticmethod
    def get_private_dataset_syftbox_url(
        datasite_email: str, dataset_name: str, path: Union[Path, str]
    ) -> str:
        return (
            f"syft://{datasite_email}/private/datasets/{dataset_name}/{Path(path).name}"
        )

    @staticmethod
    def get_readme_syftbox_url(
        datasite_email: str, dataset_name: str, readme_path: Union[Path, str]
    ) -> SyftBoxURL:
        return SyftBoxURL(
            f"syft://{datasite_email}/public/datasets/{dataset_name}/{Path(readme_path).name}"
        )


class DatasetPathManager:
    def __init__(self, syftbox_client: SyftBoxClient, host: str):
        self._syftbox_client = syftbox_client
        self._host = host

    def get_local_public_dataset_dir(self, dataset_name: str) -> Path:
        return self._syftbox_client.my_datasite / "public" / "datasets" / dataset_name

    def get_remote_public_dataset_dir(self, dataset_name: str) -> Path:
        return (
            self._syftbox_client.datasites
            / self._host
            / "public"
            / "datasets"
            / dataset_name
        )

    def get_syftbox_private_dataset_dir(self, dataset_name: str) -> Path:
        return (
            self._syftbox_client.datasites
            / self._host
            / "private"
            / "datasets"
            / dataset_name
        )

    def get_remote_public_datasets_dir(self) -> Path:
        return self._syftbox_client.datasites / self._host / "public" / "datasets"

    @property
    def syftbox_client_email(self) -> str:
        return self._syftbox_client.email

    def check_path_exists(self, path: Union[str, Path]):
        if not Path(path).exists():
            raise ValueError(f"Path does not exist: {path}")

    def check_are_both_dirs_or_files(
        self, path: Union[str, Path], mock_path: Union[str, Path]
    ):
        path, mock_path = Path(path), Path(mock_path)
        if not (
            (path.is_dir() and mock_path.is_dir())
            or (path.is_file() and mock_path.is_file())
        ):
            raise ValueError(f"Paths are not in the same type: {path} and {mock_path}")


class DatasetFilesManager:
    def __init__(self, path_manager: DatasetPathManager) -> None:
        self._path_manager = path_manager

    def check_same_file_extensions(
        self, path: Union[str, Path], mock_path: Union[str, Path]
    ):
        path, mock_path = Path(path), Path(mock_path)
        if (path.is_file() and mock_path.is_file()) and (
            not path.suffix == mock_path.suffix
        ):
            raise ValueError(f"Files must have same extension: {path} and {mock_path}")

    def check_file_extensions_for_dir(self, path: Union[str, Path], file_type: str):
        """
        Check all the file extensions in the dir and compare if they are in the list of file extensions
        """
        pass

    @staticmethod
    def _copy_file_or_dir(src: Union[str, Path], dest_dir: Path) -> Path:
        src_path = Path(src)
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / src_path.name

        if src_path.is_dir():
            shutil.copytree(src_path, dest_path, dirs_exist_ok=True)
        else:
            shutil.copy2(src_path, dest_path)
        return dest_path

    def copy_mock_to_public_syftbox_dir(
        self, dataset_name: str, mock_path: Union[str, Path]
    ) -> Path:
        public_dataset_dir: Path = self._path_manager.get_local_public_dataset_dir(
            dataset_name
        )
        return self._copy_file_or_dir(mock_path, public_dataset_dir)

    def copy_private_to_private_syftbox_dir(
        self,
        dataset_name: str,
        path: Union[str, Path],
    ) -> Path:
        private_dataset_dir: Path = self._path_manager.get_syftbox_private_dataset_dir(
            dataset_name
        )
        return self._copy_file_or_dir(path, private_dataset_dir)

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
    def __init__(self, path_manager: DatasetPathManager) -> None:
        self._path_manager = path_manager
        self._spec_store = RDSStore(
            spec=DatasetSpec,
            client=self._path_manager._syftbox_client,
            datasite=self._path_manager._host,
        )

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
        dataset_spec = DatasetSpec(
            name=dataset_create.name,
            data=private_url,
            mock=mock_url,
            file_type=dataset_create.file_type,
            tags=dataset_create.tags,
            summary=dataset_create.summary,
            readme=readme_url,
        )
        self._spec_store.create(dataset_spec)
        return dataset_spec

    def delete(self, name: str) -> bool:
        queried_result = self._spec_store.query(name=name)
        if not queried_result:
            return False
        return self._spec_store.delete(queried_result[0].id)

    def get_one(self, name: str) -> DatasetSpec:
        queried_result = self._spec_store.query(name=name)
        if not queried_result:
            raise ValueError(f"Dataset with name '{name}' not found")
        if len(queried_result) > 1:
            raise ValueError(f"Multiple datasets found with name '{name}'")
        return queried_result[0]

    def get_all(self) -> list[DatasetSpec]:
        return self._spec_store.list_all()


def ensure_is_admin(func):
    """
    Decorator to ensure the user is an admin before executing a function.
    Admin status is determined by comparing the SyftBox client email with the configured host.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self._syftbox_client.email != self._config.host:
            raise PermissionError(
                f"You must be the datasite admin to perform this operation. "
                f"Your SyftBox email: '{self._syftbox_client.email}'. "
                f"Host email: '{self._config.host}'"
            )
        return func(self, *args, **kwargs)

    return wrapper


class DatasetRDSClient:
    """
    For DatasetRDSClient, everything is done locally on the client side.
    Hence, there is no need for utilizing RPC Connections and the RPCClient like other RDS clients
    """

    def __init__(self, config: RDSClientConfig, syftbox_client: SyftBoxClient):
        self._config = config
        self._syftbox_client = syftbox_client
        self._path_manager = DatasetPathManager(self._syftbox_client, self._config.host)
        self._schema_manager = DatasetSchemaManager(self._path_manager)
        self._files_manager = DatasetFilesManager(self._path_manager)

    def _validate_before_create(
        self,
        name: str,
        path: Union[str, Path],
        mock_path: Union[str, Path],
        file_type: str,
    ) -> None:
        if (
            self._path_manager.get_local_public_dataset_dir(name).exists()
            or self._path_manager.get_syftbox_private_dataset_dir(name).exists()
        ):
            raise ValueError(f"Dataset with name '{name}' already exists")
        # validate paths
        self._path_manager.check_path_exists(path)
        self._path_manager.check_path_exists(mock_path)
        self._path_manager.check_are_both_dirs_or_files(path, mock_path)
        # validate dataset file extensions
        self._files_manager.check_same_file_extensions(path, mock_path)
        self._files_manager.check_file_extensions_for_dir(path, file_type)
        self._files_manager.check_file_extensions_for_dir(mock_path, file_type)

    def _dataset_spec_to_dataset(self, dataset_spec: DatasetSpec) -> Dataset:
        mock_path = dataset_spec.mock.to_local_path(
            datasites_path=self._syftbox_client.datasites
        )
        private_path = dataset_spec.data.to_local_path(
            datasites_path=self._syftbox_client.datasites
        )
        if dataset_spec.readme:
            description_path = dataset_spec.readme.to_local_path(
                datasites_path=self._syftbox_client.datasites
            )
        else:
            description_path = None
        return Dataset(
            uid=dataset_spec.id,
            name=dataset_spec.name,
            data_path=private_path,
            mock_path=mock_path,
            file_type=dataset_spec.file_type,
            summary=dataset_spec.summary,
            description_path=description_path,
            tags=dataset_spec.tags,
        )

    @ensure_is_admin
    def create(
        self,
        name: str,
        path: Union[str, Path],
        mock_path: Union[str, Path],
        file_type: str,
        summary: Optional[str] = None,
        description_path: Optional[str] = None,
        tags: list[str] = [],
    ) -> Dataset:
        # input types validation
        dataset_create = DatasetCreate(
            name=name,
            path=str(path),
            mock_path=str(mock_path),
            file_type=file_type,
            summary=summary,
            description_path=description_path,
            tags=tags,
        )
        # validate paths and file extensions
        self._validate_before_create(name, path, mock_path, file_type)
        try:
            self._files_manager.copy_mock_to_public_syftbox_dir(name, mock_path)
            self._files_manager.copy_description_file_to_public_syftbox_dir(
                name, description_path
            )
            self._files_manager.copy_private_to_private_syftbox_dir(name, path)
            dataset_spec = self._schema_manager.create(dataset_create)
            return self._dataset_spec_to_dataset(dataset_spec)
        except Exception as e:
            self._files_manager.cleanup_dataset_files(name)
            raise RuntimeError(f"Failed to create dataset '{name}': {str(e)}") from e

    def get(self, name: str) -> Dataset:
        queried_result: DatasetSpec = self._schema_manager.get_one(name=name)
        return self._dataset_spec_to_dataset(queried_result)

    def get_all(self) -> list[Dataset]:
        queried_results = self._schema_manager.get_all()
        return [
            self._dataset_spec_to_dataset(dataset_spec)
            for dataset_spec in queried_results
        ]

    @ensure_is_admin
    def delete(self, name: str) -> bool:
        """Delete a dataset by name.

        Args:
            name: Name of the dataset to delete

        Returns:
            True if deletion was successful

        Raises:
            RuntimeError: If deletion fails due to file system errors
        """
        try:
            schema_deleted = self._schema_manager.delete(name)
            if schema_deleted:
                self._files_manager.cleanup_dataset_files(name)
                return True
            return False
        except Exception as e:
            raise RuntimeError(f"Failed to delete dataset '{name}'") from e

    @ensure_is_admin
    def update(self, dataset_update: DatasetUpdate) -> Dataset:
        raise NotImplementedError("Dataset update is not supported yet")
