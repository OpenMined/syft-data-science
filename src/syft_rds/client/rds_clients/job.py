from pathlib import Path
from uuid import UUID

from loguru import logger

from syft_rds.client.exceptions import RDSValidationError
from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.client.utils import PathLike
from syft_rds.models import (
    Job,
    JobCreate,
    JobStatus,
    JobUpdate,
    UserCode,
)
from syft_rds.models.job_models import JobErrorKind, JobResults


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

    def create(
        self,
        user_code: UserCode | UUID,
        dataset_name: str,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
    ) -> Job:
        # TODO ref dataset by UID instead of name
        user_code_id = self._resolve_usercode_id(user_code)

        job_create = JobCreate(
            name=name,
            description=description,
            tags=tags if tags is not None else [],
            user_code_id=user_code_id,
            dataset_name=dataset_name,
        )
        job = self.rpc.job.create(job_create)

        return job

    def _get_results_from_dir(
        self,
        job: Job,
        results_dir: PathLike,
    ) -> JobResults:
        """Get the job results from the specified directory, and format it into a JobResults object."""
        results_dir = Path(results_dir)
        if not results_dir.exists():
            raise ValueError(
                f"Results directory {results_dir} does not exist for job {job.uid}"
            )

        output_dir = results_dir / "output"
        logs_dir = results_dir / "logs"
        expected_layout_msg = (
            f"{results_dir} should contain 'output' and 'logs' directories."
        )
        if not output_dir.exists():
            raise ValueError(
                f"Output directory {output_dir.name} does not exist for job {job.uid}. "
                + expected_layout_msg
            )
        if not logs_dir.exists():
            raise ValueError(
                f"Logs directory {logs_dir.name} does not exist for job {job.uid}. "
                + expected_layout_msg
            )

        return JobResults(
            job=job,
            results_dir=results_dir,
        )

    def review_results(
        self, job: Job, output_dir: PathLike | None = None
    ) -> JobResults:
        if output_dir is None:
            output_dir = self.config.runner_config.job_output_folder / job.uid.hex
        return self._get_results_from_dir(job, output_dir)

    def share_results(self, job: Job) -> None:
        if not self.is_admin:
            raise RDSValidationError("Only admins can share results")
        job_output_folder = self.config.runner_config.job_output_folder / job.uid.hex
        output_path = self.local_store.job.share_result_files(job, job_output_folder)
        updated_job = self.rpc.job.update(
            JobUpdate(
                uid=job.uid,
                status=JobStatus.shared,
                error=job.error,
            )
        )
        job.apply_update(updated_job, in_place=True)
        logger.info(f"Shared results for job {job.uid} at {output_path}")

    def get_results(self, job: Job) -> JobResults:
        """Get the shared job results."""
        if job.status != JobStatus.shared:
            raise RDSValidationError(
                f"Job {job.uid} is not shared. Current status: {job.status}"
            )
        return self._get_results_from_dir(job, job.output_path)

    def reject(self, job: Job, reason: str = "Unspecified") -> None:
        if not self.is_admin:
            raise RDSValidationError("Only admins can reject jobs")

        allowed_statuses = (
            JobStatus.pending_code_review,
            JobStatus.job_run_finished,
            JobStatus.job_run_failed,
        )
        if self.status not in allowed_statuses:
            raise ValueError(f"Cannot reject job with status: {self.status}")

        error = (
            JobErrorKind.failed_code_review
            if job.status == JobStatus.pending_code_review
            else JobErrorKind.failed_output_review
        )

        job_update = JobUpdate(
            uid=job.uid,
            status=JobStatus.rejected,
            error=error,
            error_message=reason,
        )

        updated_job = self.rpc.job.update(job_update)
        job.apply_update(updated_job, in_place=True)
