import json
import shutil
import tempfile
from pathlib import Path
from typing import Any
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
    Runtime,
    UserCode,
)
from syft_rds.models.custom_function_models import CustomFunction
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
        custom_function: CustomFunction | UUID | None = None,
        runtime_name: str | None = None,
        runtime_kind: str | None = None,
        runtime_config: dict | None = None,
    ) -> Job:
        """`submit` is a convenience method to create both a UserCode and a Job in one call."""
        if custom_function is not None:
            custom_function_id = self._resolve_custom_func_id(custom_function)
            custom_function = (
                self.rds.custom_function.get(uid=custom_function_id)
                if custom_function_id
                else None
            )
            if entrypoint is not None:
                raise RDSValidationError(
                    "Cannot specify entrypoint when using a custom function."
                )
            entrypoint = custom_function.entrypoint

        user_code = self.rds.user_code.create(
            code_path=user_code_path, entrypoint=entrypoint
        )

        runtime = self.rds.runtime.create(
            runtime_name=runtime_name,
            runtime_kind=runtime_kind,
            config=runtime_config,
        )

        job = self.create(
            name=name,
            description=description,
            user_code=user_code,
            dataset_name=dataset_name,
            tags=tags,
            custom_function=custom_function,
            runtime=runtime,
        )

        return job

    def submit_with_params(
        self,
        dataset_name: str,
        custom_function: CustomFunction | UUID,
        **params: Any,
    ) -> Job:
        """
        Utility method to a job with parameters for a custom function.

        Args:
            dataset_name (str): The name of the dataset to use.
            custom_function (CustomFunction | UUID): The custom function to use.
            **params: Additional parameters to pass to the custom function.

        Returns:
            Job: The created job.
        """
        if isinstance(custom_function, UUID):
            custom_function = self.rds.custom_function.get(uid=custom_function)
        elif not isinstance(custom_function, CustomFunction):
            raise ValueError(
                f"Invalid custom_function type {type(custom_function)}. Must be CustomFunction or UUID"
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_dir_path = Path(tmpdir)
            user_params_path = tmp_dir_path / custom_function.input_params_filename
            if not user_params_path.suffix == ".json":
                raise ValueError(
                    f"Input params file must be a JSON file, got {user_params_path.suffix}. Please contact the administrator."
                )

            try:
                params_json = json.dumps(params)
            except Exception as e:
                raise ValueError(f"Failed to serialize params to JSON: {e}.") from e

            user_params_path.write_text(params_json)

            return self.submit(
                user_code_path=user_params_path,
                dataset_name=dataset_name,
                custom_function=custom_function,
            )

    def _resolve_custom_func_id(
        self, custom_function: CustomFunction | UUID | None
    ) -> UUID | None:
        if custom_function is None:
            return None
        if isinstance(custom_function, UUID):
            return custom_function
        elif isinstance(custom_function, CustomFunction):
            return custom_function.uid
        else:
            raise RDSValidationError(
                f"Invalid custom_function type {type(custom_function)}. Must be CustomFunction, UUID, or None"
            )

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
        runtime: Runtime,
        name: str | None = None,
        description: str | None = None,
        tags: list[str] | None = None,
        custom_function: CustomFunction | UUID | None = None,
    ) -> Job:
        # TODO ref dataset by UID instead of name
        user_code_id = self._resolve_usercode_id(user_code)
        custom_function_id = self._resolve_custom_func_id(custom_function)

        job_create = JobCreate(
            name=name,
            description=description,
            tags=tags if tags is not None else [],
            user_code_id=user_code_id,
            runtime_id=runtime.uid,
            dataset_name=dataset_name,
            custom_function_id=custom_function_id,
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
        job_results_folder = self.config.runner_config.job_output_folder / job.uid.hex
        output_path = self._share_result_files(job, job_results_folder)
        updated_job = self.rpc.job.update(
            JobUpdate(
                uid=job.uid,
                status=JobStatus.shared,
                error=job.error,
            )
        )
        job.apply_update(updated_job, in_place=True)
        logger.info(f"Shared results for job {job.uid} at {output_path}")

    def _share_result_files(self, job: Job, job_results_folder: Path) -> Path:
        syftbox_output_path = job.output_url.to_local_path(
            self.rds._syftbox_client.datasites
        )
        if not syftbox_output_path.exists():
            syftbox_output_path.mkdir(parents=True)

        # Copy all contents from job_output_folder to the output path
        for item in job_results_folder.iterdir():
            if item.is_file():
                shutil.copy2(item, syftbox_output_path)
            elif item.is_dir():
                shutil.copytree(
                    item,
                    syftbox_output_path / item.name,
                    dirs_exist_ok=True,
                )

        return syftbox_output_path

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

    def update_job_status(self, job_update: JobUpdate, job: Job) -> Job:
        new_job = self.rpc.job.update(job_update)
        return job.apply_update(new_job)
