from pathlib import Path
from uuid import UUID

from loguru import logger

from syft_rds.client.exceptions import RDSValidationError
from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.client.utils import PathLike
from syft_rds.models.models import (
    Job,
    JobCreate,
    JobStatus,
    JobUpdate,
    UserCode,
)
from syft_core import Client


class JobRDSClient(RDSClientModule[Job]):
    ITEM_TYPE = Job

    def submit(
        self,
        user_code_path: PathLike,
        dataset_name: str,
        entrypoint: str | None = None,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        enclave: str = "",
    ) -> Job:
        """`submit` is a convenience method to create both a UserCode and a Job in one call."""
        user_code = self.rds.user_code.create(
            code_path=user_code_path, entrypoint=entrypoint
        )
        job = self.create(
            name=name,
            description=description,
            user_code=user_code,
            dataset_name=dataset_name,
            tags=tags,
            enclave=enclave,
        )

        return job

    def _resolve_usercode_id(self, user_code: UserCode | UUID) -> UUID:
        if isinstance(user_code, UUID):
            return user_code
        elif isinstance(user_code, UserCode):
            return user_code.uid
        else:
            raise RDSValidationError(
                f"Invalid user_code type {type(user_code)}. Must be UserCode, UUID, or str"
            )

    def _verify_enclave(self, enclave: str) -> None:
        """Verify that the enclave is valid."""
        client: Client = self.rpc.connection.sender_client
        enclave_app_dir = client.app_data("enclave", datasite=enclave)
        public_key_path = enclave_app_dir / "keys" / "public_key.pem"
        if not public_key_path.exists():
            raise RDSValidationError(
                f"Enclave {enclave} does not exist or is not valid. "
                f"Public key file {public_key_path} not found."
            )

    def create(
        self,
        user_code: UserCode | UUID,
        dataset_name: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        enclave: str = "",
    ) -> Job:
        # TODO ref dataset by UID instead of name
        user_code_id = self._resolve_usercode_id(user_code)

        if enclave:
            self._verify_enclave(enclave)

        job_create = JobCreate(
            name=name,
            description=description,
            tags=tags if tags is not None else [],
            user_code_id=user_code_id,
            dataset_name=dataset_name,
            enclave=enclave,
        )
        job = self.rpc.jobs.create(job_create)

        return job

    def share_results(self, job: Job) -> tuple[Path, Job]:
        if not self.is_admin:
            raise RDSValidationError("Only admins can share results")
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
        return output_path, job.apply_update(updated_job)

    def approve(self, job: Job) -> Job:
        if not self.is_admin:
            raise RDSValidationError("Only admins can approve jobs")
        job_update = job.get_update_for_approve()
        updated_job = self.rpc.jobs.update(job_update)
        job.apply_update(updated_job)
        return job

    def reject(self, job: Job, reason: str = "Unspecified") -> Job:
        if not self.is_admin:
            raise RDSValidationError("Only admins can reject jobs")
        job_update = job.get_update_for_reject(reason)
        updated_job = self.rpc.jobs.update(job_update)
        job.apply_update(updated_job)
        return job
