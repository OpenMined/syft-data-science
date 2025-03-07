from pathlib import Path
from typing import Callable
from uuid import UUID
from loguru import logger
from syft_rds.client.exceptions import RDSValidationError
from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.client.utils import PathLike
from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
    Job,
    JobCreate,
    JobStatus,
    JobUpdate,
    UserCodeCreate,
)


class JobRDSClient(RDSClientModule):
    def submit(
        self,
        name: str | None = None,
        description: str | None = None,
        user_code_path: PathLike | None = None,
        dataset_name: str | None = None,
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
            description=description,
            user_code_id=user_code.uid,
            tags=tags if tags is not None else [],
            dataset_name=dataset_name,
        )
        if name is not None:
            job_create.name = name
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

    def share_results(self, job: Job) -> Path:
        job_output_folder = self.config.runner_config.job_output_folder / job.uid.hex
        output_path = self.local_store.jobs.share_result_files(job, job_output_folder)
        updated_job = self.rpc.jobs.update(
            JobUpdate(
                uid=job.uid,
                status=JobStatus.shared,
                error=job.error,
            )
        )
        logger.info(f"Shared results for job {job.uid} at {output_path}")
        return output_path, job.apply(updated_job)

    def reject(self, job: Job, reason: str = "Unspecified") -> Job:
        job_update = job.get_update_for_reject(reason)
        updated_job = self.rpc.jobs.update(job_update)
        job.apply(updated_job)
        return job
