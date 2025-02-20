from uuid import UUID

from pydantic import BaseModel

from syft_rds.client.exceptions import RDSValidationError
from syft_rds.client.rpc_client import RPCClient
from syft_rds.client.utils import PathLike
from syft_rds.models.models import Job, JobCreate, UserCodeCreate


def init_session(host: str) -> "RDSClient":
    config = RDSClientConfig(
        host=host,
    )
    return RDSClient(config)


class RDSClientConfig(BaseModel):
    host: str


class RDSClientModule:
    def __init__(self, config: RDSClientConfig, rpc_client: RPCClient | None = None):
        self.config = config
        self.rpc = rpc_client or RPCClient(config)


class RDSClient(RDSClientModule):
    def __init__(self, config: RDSClientConfig):
        super().__init__(config)
        self.jobs = JobRDSClient(self.config, self.rpc)
        self.runtime = RuntimeRDSClient(self.config, self.rpc)


class JobRDSClient(RDSClient):
    def submit(
        self,
        name: str,
        description: str | None = None,
        runtime: str | None = None,
        user_code_path: PathLike | None = None,
        output_path: PathLike | None = None,
        function: callable | None = None,
        function_args: list = None,
        function_kwargs: dict = None,
        tags: list[str] | None = None,
    ) -> Job:
        # Validate UserCode and submit
        user_code_create = self._create_usercode_from_submit(
            user_code_path=user_code_path,
            function=function,
            function_args=function_args,
            function_kwargs=function_kwargs,
        )

        user_code = self.rpc.user_code.create(user_code_create)

        # Validate Job and submit
        job_create = JobCreate(
            name=name,
            description=description,
            runtime=runtime,
            user_code_id=user_code.uid,
            tags=tags,
            output_path=output_path,
        )
        job = self.rpc.jobs.create(job_create)
        job._client_cache[user_code.uid] = user_code

        return job

    def _create_usercode_from_submit(
        self,
        user_code_path: str | None = None,
        function: callable | None = None,
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
            user_code = UserCodeCreate.from_func(
                function=function,
                function_args=function_args,
                function_kwargs=function_kwargs,
            )

        else:
            raise RDSValidationError(
                "You must provide either a user_code_path or a function, function_args, and function_kwargs"
            )

    def get_all(self) -> list[Job]:
        return self.rpc.jobs.get_all()

    def get(self, uid: UUID | None = None, name: str | None = None) -> Job:
        if uid and name:
            raise RDSValidationError(
                "You must provide either a uid or a name, not both."
            )
        if uid is not None:
            return self.rpc.jobs.get(uid)
        elif name is not None:
            return self.rpc.jobs.get_one(name)


class RuntimeRDSClient(RDSClient):
    def get_all(self) -> list[str]:
        return self.rpc.runtime.get_all()

    def create(self, name: str) -> str:
        return self.rpc.runtime.create(name)
