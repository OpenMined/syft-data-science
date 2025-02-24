from typing import Callable
from uuid import UUID

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
        self.data = DatasetRDSClient(self.config, self.rpc)


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
    def create(self, name: str) -> str:
        return self.rpc.dataset.create(name)

    def get_all(self) -> list[str]:
        return self.rpc.dataset.get_all()
