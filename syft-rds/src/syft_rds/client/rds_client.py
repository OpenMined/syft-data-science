from pathlib import Path
from typing import Callable, Optional, Union
from uuid import UUID
import shutil
from loguru import logger
import json

from pydantic import BaseModel

from syft_event import SyftEvents

from syft_rds.client.connection import get_connection
from syft_rds.client.exceptions import RDSValidationError
from syft_rds.client.rpc_client import RPCClient
from syft_rds.client.utils import PathLike
from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
    Job,
    JobCreate,
    UserCodeCreate,
    DatasetCreate,
    Dataset,
)


def init_session(host: str, mock_server: SyftEvents | None = None) -> "RDSClient":
    """
    Initialize a session with the RDSClient.

    If `mock_server` is provided, a in-process RPC connection will be used.

    Args:
        host (str):
        mock_server (SyftEvents, optional): The server we're connecting to.
            Client will use a mock, in-process RPC connection if provided.
            Defaults to None.

    Returns:
        RDSClient: The RDSClient instance.
    """

    # Implementation note: All dependencies are initiated here so we can inject and mock them in tests.
    config = RDSClientConfig(host=host)
    connection = get_connection(mock_server)
    rpc_client = RPCClient(config, connection)
    return RDSClient(config, rpc_client)


class RDSClientConfig(BaseModel):
    host: str
    app_name: str = "RDS"
    default_runtime: str = "python"

    rpc_expiry: str = "5m"
    rpc_cache: bool = True


class RDSClientModule:
    def __init__(self, config: RDSClientConfig, rpc_client: RPCClient):
        self.config = config
        self.rpc = rpc_client

    def set_default_runtime(self, runtime: str):
        self.config.default_runtime = runtime


class RDSClient(RDSClientModule):
    def __init__(self, config: RDSClientConfig, rpc_client: RPCClient):
        super().__init__(config, rpc_client)
        self.jobs = JobRDSClient(self.config, self.rpc)
        self.runtime = RuntimeRDSClient(self.config, self.rpc)
        self.dataset = DatasetRDSClient(self.config, self.rpc)


class JobRDSClient(RDSClientModule):
    def submit(
        self,
        name: str,
        description: str | None = None,
        runtime: str | None = None,
        user_code_path: PathLike | None = None,
        output_path: PathLike | None = None,
        function: Callable | None = None,
        function_args: list = None,
        function_kwargs: dict = None,
        tags: list[str] | None = None,
    ) -> Job:
        user_code_create = self._create_usercode(
            user_code_path=user_code_path,
            function=function,
            function_args=function_args,
            function_kwargs=function_kwargs,
        )

        user_code = self.rpc.user_code.create(user_code_create)

        job_create = JobCreate(
            name=name,
            description=description,
            runtime=runtime or self.config.default_runtime,
            user_code_id=user_code.uid,
            tags=tags if tags is not None else [],
            output_path=output_path,
        )
        job = self.rpc.jobs.create(job_create)
        job._client_cache[user_code.uid] = user_code

        return job

    def _create_usercode(
        self,
        user_code_path: str | None = None,
        function: Callable | None = None,
        function_args: list = None,
        function_kwargs: dict = None,
    ) -> UserCodeCreate:
        if user_code_path is not None:
            user_code = UserCodeCreate(path=user_code_path)
            return user_code

        elif (
            function is not None
            and function_args is not None
            and function_kwargs is not None
        ):
            raise NotImplementedError(
                "Creating UserCode from function is not implemented yet"
            )

        else:
            raise RDSValidationError(
                "You must provide either a user_code_path or a function, function_args, and function_kwargs"
            )

    def get_all(self) -> list[Job]:
        return self.rpc.jobs.get_all(GetAllRequest())

    def get(self, uid: UUID) -> Job:
        return self.rpc.jobs.get_one(GetOneRequest(uid=uid))


class RuntimeRDSClient(RDSClientModule):
    def create(self, name: str) -> str:
        return self.rpc.runtime.create(name)

    def get_all(self) -> list[str]:
        return self.rpc.runtime.get_all()


class DatasetRDSClient(RDSClientModule):
    def raise_error_if_not_admin(self):
        if self._syftbox_client.email != self._config.host:
            raise ValueError(
                f"SyftBox email and RDS host must be the same to create a dataset "
                f"(no remote dataset creation for now). "
                f"SyftBox email: {self._syftbox_client.email}. "
                f"Host email: {self._config.host}"
            )

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
        if (
            self.get_syftbox_public_dataset_dir(name).exists()
            or self.get_syftbox_private_dataset_dir(name).exists()
        ):
            raise ValueError(f"Dataset with name '{name}' already exists")

        # validate paths
        check_path_exists(path)
        check_path_exists(mock_path)
        check_are_both_dirs_or_files(path, mock_path)

        # validate dataset file extensions
        check_same_file_extensions(path, mock_path)
        check_file_extensions_for_dir(path, file_type)
        check_file_extensions_for_dir(mock_path, file_type)

        # validate input types
        dataset_create = DatasetCreate(
            name=name,
            path=str(path),
            mock_path=str(mock_path),
            file_type=file_type,
            summary=summary,
            description_path=description_path,
        )

        try:
            mock_syftbox_path = self._copy_mock_to_public_syftbox_dir(name, mock_path)
            description_file_syftbox_path = self._copy_desc_file_to_public_syftbox_dir(
                name, description_path
            )
            private_syftbox_path = self._copy_private_to_private_syftbox_dir(name, path)
            self._generate_dataset_schema(dataset_create)

            return Dataset(
                name=name,
                path=str(private_syftbox_path),
                mock_path=str(mock_syftbox_path),
                file_type=file_type,
                summary=summary,
                description_path=str(description_file_syftbox_path),
            )
        except Exception as e:
            self._cleanup_dataset_files(name)
            raise RuntimeError(f"Failed to create dataset '{name}': {str(e)}") from e

    def get(self) -> Dataset:
        pass

    def delete(self, name: str) -> None:
        try:
            self._cleanup_dataset_files(name)
        except Exception as e:
            raise RuntimeError(f"Failed to delete dataset '{name}': {str(e)}") from e

    def _cleanup_dataset_files(self, name: str) -> None:
        try:
            public_dir = self.get_syftbox_public_dataset_dir(name)
            private_dir = self.get_syftbox_private_dataset_dir(name)
            shutil.rmtree(public_dir)
            shutil.rmtree(private_dir)
        except Exception as e:
            logger.error(f"Failed to cleanup dataset files: {str(e)}")
            raise RuntimeError(f"Failed to clean up dataset '{name}': {str(e)}") from e

    def _copy_mock_to_public_syftbox_dir(
        self, dataset_name: str, mock_path: Union[str, Path]
    ) -> Path:
        public_dataset_dir: Path = self.get_syftbox_public_dataset_dir(dataset_name)
        public_dataset_dir.mkdir(parents=True, exist_ok=True)
        mock_path = Path(mock_path)
        dest_path = public_dataset_dir / mock_path.name
        if mock_path.is_dir():
            shutil.copytree(mock_path, dest_path, dirs_exist_ok=True)
        else:
            shutil.copy2(mock_path, dest_path)
        return dest_path

    def _copy_desc_file_to_public_syftbox_dir(
        self, dataset_name: str, description_path: Union[str, Path]
    ) -> Path:
        public_dataset_dir: Path = self.get_syftbox_public_dataset_dir(dataset_name)
        dest_path = public_dataset_dir / Path(description_path).name
        shutil.copy2(description_path, public_dataset_dir / Path(description_path).name)
        return dest_path

    def get_syftbox_public_dataset_dir(self, dataset_name: str) -> Path:
        return self._syftbox_client.my_datasite / "public" / "datasets" / dataset_name

    def _copy_private_to_private_syftbox_dir(
        self,
        dataset_name: str,
        path: Union[str, Path],
    ) -> Path:
        private_dataset_dir: Path = self.get_syftbox_private_dataset_dir(dataset_name)
        private_dataset_dir.mkdir(parents=True, exist_ok=True)
        prv_path = Path(path)
        dest_path = private_dataset_dir / prv_path.name
        if prv_path.is_dir():
            shutil.copytree(prv_path, dest_path, dirs_exist_ok=True)
        else:
            shutil.copy2(prv_path, dest_path)
        return dest_path

    def get_syftbox_private_dataset_dir(self, dataset_name: str):
        return (
            self._syftbox_client.workspace.data_dir
            / "private"
            / "datasets"
            / dataset_name
        )

    def _generate_dataset_schema(self, dataset_create: DatasetCreate) -> None:
        mock_path = Path(dataset_create.mock_path)
        prv_path = Path(dataset_create.path)
        # Build schema
        schema_dict = dataset_create.model_dump()
        schema_dict.pop("path")
        schema_dict.pop("mock_path")
        schema_dict.pop("description_path")
        schema_dict["mock"] = self.get_syftbox_mock_dataset_url(
            dataset_create.name, mock_path
        )
        schema_dict["private"] = self.get_syftbox_private_dataset_url(
            dataset_create.name, prv_path
        )
        schema_dict["readme"] = self.get_syftbox_mock_dataset_url(
            dataset_create.name, dataset_create.description_path
        )
        # Write schema
        public_dataset_dir: Path = self.get_syftbox_public_dataset_dir(
            dataset_create.name
        )
        with open(public_dataset_dir / "dataset.schema.json", "w") as f:
            json.dump(schema_dict, f, indent=2)

    def get_syftbox_mock_dataset_url(
        self, dataset_name: str, mock_path: Union[Path, str]
    ) -> str:
        return f"syft://{self._syftbox_client.email}/public/datasets/{dataset_name}/{Path(mock_path).name}"

    def get_syftbox_private_dataset_url(
        self, dataset_name: str, path: Union[Path, str]
    ) -> str:
        return f"syft://private/datasets/{dataset_name}/{Path(path).name}"


def check_path_exists(path: Union[str, Path]):
    if not Path(path).exists():
        raise ValueError(f"Paths must exist: {path}")


def check_are_both_dirs_or_files(path: Union[str, Path], mock_path: Union[str, Path]):
    path, mock_path = Path(path), Path(mock_path)
    if not (
        (path.is_dir() and mock_path.is_dir())
        or (path.is_file() and mock_path.is_file())
    ):
        raise ValueError(f"Paths must be same type: {path} and {mock_path}")


def check_same_file_extensions(path: Union[str, Path], mock_path: Union[str, Path]):
    path, mock_path = Path(path), Path(mock_path)
    if (path.is_file() and mock_path.is_file()) and (
        not path.suffix == mock_path.suffix
    ):
        raise ValueError(f"Files must have same extension: {path} and {mock_path}")


def check_file_extensions_for_dir(path: Union[str, Path], file_type: str):
    """
    Check all the file extensions in the dir and compare if they are in the list of file extensions
    """
    pass
