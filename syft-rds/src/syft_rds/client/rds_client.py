from pathlib import Path
from typing import Callable, Optional, Union
from uuid import UUID
import shutil

from pydantic import BaseModel

from syft_event import SyftEvents
from syft_core import Client as SyftBoxClient

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
    default_runtime: str = "python-3.12"

    rpc_expiry: str = "5m"
    rpc_cache: bool = True


class RDSClientModule:
    def __init__(
        self,
        host: str,
        rpc_client: Optional[RPCClient] = None,
        syftbox_client: Optional[SyftBoxClient] = None,
    ) -> None:
        self._config = RDSClientConfig(host=host)
        self._rpc = rpc_client
        self._syftbox_client = (
            syftbox_client if syftbox_client is not None else SyftBoxClient.load()
        )

    def set_default_runtime(self, runtime: str):
        self.config.default_runtime = runtime


class RDSClient(RDSClientModule):
    def __init__(self, host: str, rpc_client: Optional[RPCClient] = None):
        super().__init__(host, rpc_client)
        self.jobs = JobRDSClient(host, self._rpc)
        self.runtime = RuntimeRDSClient(host, self._rpc)
        self.dataset = DatasetRDSClient(host, self._rpc)


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
    def create(
        self,
        name: str,
        path: Union[str, Path],
        mock_path: Union[str, Path],
        summary: Optional[str] = None,
        description_path: Optional[str] = None,
    ):
        # TODO: do we have to check if self._syftbox_client.email is the same as self._config.host for now?
        DatasetCreate(
            name=name,
            path=str(path),
            mock_path=str(mock_path),
            summary=summary,
            description_path=description_path,
        )
        self.check_dataset_name_unique(name)
        check_mock_and_private_exists(path, mock_path)
        check_are_both_dirs_or_files(path, mock_path)
        check_same_file_extensions(path, mock_path)
        check_same_file_extensions_for_dir(path, mock_path)

        self._copy_mock_to_public_syftbox_dir(mock_path, name)
        self._copy_private_to_private_syftbox_dir(path, name)
        self._generate_dataset_schema_file()

    def _copy_mock_to_public_syftbox_dir(
        self, mock_path: Union[str, Path], dataset_name: str
    ):
        public_dataset_dir: Path = self.get_syftbox_public_dataset_dir(dataset_name)
        public_dataset_dir.mkdir(parents=True, exist_ok=True)
        mock_path = Path(mock_path)
        if mock_path.is_dir():
            shutil.copytree(
                mock_path, public_dataset_dir / mock_path.name, dirs_exist_ok=True
            )
        else:
            shutil.copy2(mock_path, public_dataset_dir / mock_path.name)

    def get_syftbox_public_dataset_dir(self, dataset_name: str):
        return self._syftbox_client.my_datasite / "public" / "datasets" / dataset_name

    def _copy_private_to_private_syftbox_dir(
        self, path: Union[str, Path], dataset_name: str
    ):
        private_dataset_dir: Path = self.get_syftbox_private_dataset_dir(dataset_name)
        private_dataset_dir.mkdir(parents=True, exist_ok=True)
        prv_path = Path(path)
        if prv_path.is_dir():
            shutil.copytree(
                prv_path, private_dataset_dir / prv_path.name, dirs_exist_ok=True
            )
        else:
            shutil.copy2(prv_path, private_dataset_dir / prv_path.name)

    def get_syftbox_private_dataset_dir(self, dataset_name: str):
        return (
            self._syftbox_client.workspace.data_dir
            / "private"
            / "datasets"
            / dataset_name
        )

    def _generate_dataset_schema_file(self):
        pass

    def check_dataset_name_unique(self, name: str):
        public_dataset_dir: Path = self.get_syftbox_public_dataset_dir(name)
        if public_dataset_dir.exists():
            raise ValueError(
                f"Dataset with name '{name}' already exists at {public_dataset_dir}"
            )

        private_dataset_dir: Path = self.get_syftbox_private_dataset_dir(name)
        if private_dataset_dir.exists():
            raise ValueError(
                f"Dataset with name '{name}' already exists at {private_dataset_dir}"
            )


def check_mock_and_private_exists(path: Union[str, Path], mock_path: Union[str, Path]):
    path, mock_path = Path(path), Path(mock_path)
    if not (mock_path.exists() and path.exists()):
        raise ValueError(f"Paths must exist: {mock_path} and {path}")


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


def check_same_file_extensions_for_dir(
    path: Union[str, Path], mock_path: Union[str, Path]
):
    # check file extension for dirs, but how?
    # Since we do not know exactly the depth of the nested dir,
    # do we have to traverse through it?
    pass
