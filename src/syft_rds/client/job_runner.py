from pathlib import Path
from typing import TYPE_CHECKING, Optional
from uuid import UUID

from loguru import logger

from syft_rds.client.rds_clients.base import ClientRunnerConfig
from syft_rds.models import Job, JobStatus
from syft_rds.syft_runtime.main import (
    DockerRunner,
    FileOutputHandler,
    JobConfig,
    RichConsoleUI,
    TextUI,
)

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClient


class JobRunner:
    def __init__(self, rds_client: "RDSClient"):
        self.rds_client = rds_client

    @property
    def runner_config(self) -> ClientRunnerConfig:
        return self.rds_client.config.runner_config

    def get_default_config_for_job(self, job: Job) -> JobConfig:
        user_code = self.rds_client.user_code.get(job.user_code_id)
        dataset = self.rds_client.dataset.get(name=job.dataset_name)
        runtime = dataset.runtime or self.runner_config.runtime
        runner_config = self.runner_config

        return JobConfig(
            function_folder=user_code.local_dir,
            args=[user_code.entrypoint],
            data_path=dataset.get_private_path(),
            runtime=runtime,
            job_folder=runner_config.job_output_folder / job.uid.hex,
            timeout=runner_config.timeout,
            use_docker=runner_config.use_docker,
        )

    def _prepare_custom_function(
        self,
        code_dir: Path,
        custom_function_id: UUID,
    ) -> None:
        custom_function = self.rds_client.custom_function.get(uid=custom_function_id)
        # Copy all files from the custom function to the code directory
        import shutil

        for file in custom_function.local_dir.glob("*"):
            if file.is_file():
                shutil.copy2(file, code_dir / file.name)
            elif file.is_dir():
                shutil.copytree(file, code_dir / file.name, dirs_exist_ok=True)

    def _get_display_handler(
        self, display_type: str, show_stdout: bool, show_stderr: bool
    ):
        """Returns the appropriate display handler based on the display type."""
        if display_type == "rich":
            return RichConsoleUI(
                show_stdout=show_stdout,
                show_stderr=show_stderr,
            )
        elif display_type == "text":
            return TextUI(
                show_stdout=show_stdout,
                show_stderr=show_stderr,
            )
        else:
            raise ValueError(f"Unknown display type: {display_type}")

    def _run(
        self,
        config: JobConfig,
        display_type: str = "text",
        show_stdout: bool = True,
        show_stderr: bool = True,
    ) -> int:
        """Runs a job.

        Args:
            job (Job): The job to run
        """

        display_handler = self._get_display_handler(
            display_type=display_type,
            show_stdout=show_stdout,
            show_stderr=show_stderr,
        )

        runner = DockerRunner(handlers=[FileOutputHandler(), display_handler])
        return_code = runner.run(config)
        return return_code

    def run_private(
        self,
        job: Job,
        config: Optional[JobConfig] = None,
        display_type: str = "text",
        show_stdout: bool = True,
        show_stderr: bool = True,
    ) -> Job:
        if job.status == JobStatus.rejected:
            raise ValueError(
                "Cannot run rejected job, "
                "if you want to override this, "
                "set job.status to something else"
            )

        config = config or self.get_default_config_for_job(job)

        logger.warning("Running job without docker is not secure")
        return_code = self._run(
            config=config,
            display_type=display_type,
            show_stdout=show_stdout,
            show_stderr=show_stderr,
        )
        job_update = job.get_update_for_return_code(return_code)
        new_job = self.rds_client.rpc.job.update(job_update)
        return job.apply_update(new_job)

    def run_mock(
        self,
        job: Job,
        config: Optional[JobConfig] = None,
        display_type: str = "text",
        show_stdout: bool = True,
        show_stderr: bool = True,
    ) -> Job:
        config = config or self.get_default_config_for_job(job)
        config.data_path = self.rds_client.dataset.get(
            name=job.dataset_name
        ).get_mock_path()
        self._run(
            config=config,
            display_type=display_type,
            show_stdout=show_stdout,
            show_stderr=show_stderr,
        )
        return job
