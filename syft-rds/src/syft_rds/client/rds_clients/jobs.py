from typing import Callable
from uuid import UUID

from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.client.exceptions import RDSValidationError
from syft_rds.client.utils import PathLike
from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
    Job,
    JobCreate,
    UserCodeCreate,
)


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
