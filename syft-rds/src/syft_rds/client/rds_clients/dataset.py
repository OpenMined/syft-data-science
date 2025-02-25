from pathlib import Path
from typing import Optional, Union
import shutil
from loguru import logger
import json

from syft_core import Client as SyftBoxClient

from syft_rds.client.rpc_client import RPCClient
from syft_rds.client.rds_clients.base import RDSClientConfig, RDSClientModule
from syft_rds.models.models import DatasetCreate, Dataset, DatasetUpdate


class DatasetUrlManager:
    @staticmethod
    def get_syftbox_mock_dataset_url(
        datasite_email: str, dataset_name: str, mock_path: Union[Path, str]
    ) -> str:
        return f"syft://{datasite_email}/public/datasets/{dataset_name}/{Path(mock_path).name}"

    @staticmethod
    def get_syftbox_private_dataset_url(
        dataset_name: str, path: Union[Path, str]
    ) -> str:
        return f"syft://private/datasets/{dataset_name}/{Path(path).name}"


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
            self._syftbox_client.workspace.data_dir
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
            raise ValueError(f"Paths must exist: {path}")

    def check_are_both_dirs_or_files(
        self, path: Union[str, Path], mock_path: Union[str, Path]
    ):
        path, mock_path = Path(path), Path(mock_path)
        if not (
            (path.is_dir() and mock_path.is_dir())
            or (path.is_file() and mock_path.is_file())
        ):
            raise ValueError(f"Paths must be same type: {path} and {mock_path}")


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
    """
    TODO: Use RDSStore instead
    """

    def __init__(self, path_manager: DatasetPathManager) -> None:
        self._path_manager = path_manager

    def create(self, dataset_create: DatasetCreate) -> None:
        mock_path = Path(dataset_create.mock_path)
        prv_path = Path(dataset_create.path)
        # Build schema
        schema_dict = dataset_create.model_dump()
        schema_dict.pop("path")
        schema_dict.pop("mock_path")
        schema_dict.pop("description_path")
        schema_dict["mock"] = DatasetUrlManager.get_syftbox_mock_dataset_url(
            self._path_manager.syftbox_client_email, dataset_create.name, mock_path
        )
        schema_dict["private"] = DatasetUrlManager.get_syftbox_private_dataset_url(
            dataset_create.name, prv_path
        )
        schema_dict["readme"] = DatasetUrlManager.get_syftbox_mock_dataset_url(
            self._path_manager.syftbox_client_email,
            dataset_create.name,
            dataset_create.description_path,
        )
        # Write schema file
        public_dataset_dir: Path = self._path_manager.get_local_public_dataset_dir(
            dataset_create.name
        )
        with open(public_dataset_dir / "dataset.schema.json", "w") as f:
            json.dump(schema_dict, f, indent=2)

    def get(self, path: Union[str, Path]) -> dict:
        with open(path, "r") as f:
            return json.load(f)


class DatasetRDSClient(RDSClientModule):
    """
    For DatasetRDSClient, everything is done on the client side
    Hence, there is no need for utilizing RPC Connections and the RPCClient
    """

    def __init__(self, config: RDSClientConfig, rpc_client: RPCClient):
        super().__init__(config, rpc_client)
        self._config = config
        self._syftbox_client = SyftBoxClient.load()
        self._path_manager = DatasetPathManager(self._syftbox_client, self._config.host)
        self._schema_manager = DatasetSchemaManager(self._path_manager)
        self._files_manager = DatasetFilesManager(self._path_manager)

    def raise_error_if_not_admin(self):
        if self._syftbox_client.email != self._config.host:
            raise ValueError(
                f"SyftBox email and RDS host must be the same to create a dataset "
                f"(no remote dataset creation for now). "
                f"SyftBox email: {self._syftbox_client.email}. "
                f"Host email: {self.config.host}"
            )

    def _validate_before_create(
        self,
        name: str,
        path: Union[str, Path],
        mock_path: Union[str, Path],
        file_type: str,
    ) -> DatasetCreate:
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

    def create(
        self,
        name: str,
        path: Union[str, Path],
        mock_path: Union[str, Path],
        file_type: str,
        summary: Optional[str] = None,
        description_path: Optional[str] = None,
    ) -> Dataset:
        self.raise_error_if_not_admin()
        dataset_create = self._validate_before_create(name, path, mock_path, file_type)
        dataset_create = DatasetCreate(
            name=name,
            path=str(path),
            mock_path=str(mock_path),
            file_type=file_type,
            summary=summary,
            description_path=description_path,
        )
        try:
            mock_syftbox_path = self._files_manager.copy_mock_to_public_syftbox_dir(
                name, mock_path
            )
            description_file_syftbox_path = (
                self._files_manager.copy_description_file_to_public_syftbox_dir(
                    name, description_path
                )
            )
            private_syftbox_path = (
                self._files_manager.copy_private_to_private_syftbox_dir(name, path)
            )
            self._schema_manager.create(dataset_create)
            return Dataset(
                name=name,
                private=str(private_syftbox_path),
                mock=str(mock_syftbox_path),
                file_type=file_type,
                summary=summary,
                description_path=str(description_file_syftbox_path),
            )
        except Exception as e:
            self._files_manager.cleanup_dataset_files(name)
            raise RuntimeError(f"Failed to create dataset '{name}': {str(e)}") from e

    def get(self, name: str) -> Dataset:
        remote_public_dataset_dir: Path = (
            self._path_manager.get_remote_public_dataset_dir(name)
        )
        if not remote_public_dataset_dir.exists():
            raise ValueError(f"Dataset '{name}' does not exist")

        schema: dict = self._schema_manager.get(
            remote_public_dataset_dir / "dataset.schema.json"
        )

        return Dataset(
            name=name,
            private=schema["private"],
            mock=str(remote_public_dataset_dir),
            file_type=schema["file_type"],
            summary=schema["summary"],
            description_path=str(
                remote_public_dataset_dir / Path(schema["readme"]).name
            ),
        )

    def get_all(self) -> list[Dataset]:
        for (
            dataset_dir
        ) in self._path_manager.get_remote_public_datasets_dir().iterdir():
            pass

    def delete(self, name: str) -> None:
        self.raise_error_if_not_admin()
        try:
            self._files_manager.cleanup_dataset_files(name)
        except Exception as e:
            raise RuntimeError(f"Failed to delete dataset '{name}': {str(e)}") from e

    def update(self, dataset_update: DatasetUpdate) -> Dataset:
        self.raise_error_if_not_admin()
        raise NotImplementedError("Dataset update is not supported yet")
