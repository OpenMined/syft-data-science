import enum
import json
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional
from uuid import UUID
from datetime import datetime

import rich
from IPython.display import HTML, display
from pydantic import BaseModel, Field, model_validator
from syft_core import SyftBoxURL

from syft_rds.display_utils.html_format import create_html_repr
from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate
from syft_rds.models.runtime_models import Runtime
from syft_rds.utils.name_generator import generate_name

if TYPE_CHECKING:
    from syft_rds.models import UserCode, Runtime


class JobStatus(str, enum.Enum):
    pending_code_review = "pending_code_review"
    job_run_failed = "job_run_failed"
    job_run_finished = "job_run_finished"
    job_in_progress = "job_in_progress"

    # end states
    rejected = "rejected"  # failed to pass the review
    shared = "shared"  # shared with the user


class JobErrorKind(str, enum.Enum):
    no_error = "no_error"
    timeout = "timeout"
    cancelled = "cancelled"
    execution_failed = "execution_failed"
    failed_code_review = "failed_code_review"
    failed_output_review = "failed_output_review"


class Job(ItemBase):
    class Config:
        extra = "forbid"

    __schema_name__ = "job"
    __table_extra_fields__ = [
        "created_by",
        "name",
        "dataset_name",
        "runtime_name",
        "status",
        "error",
    ]

    name: str = Field(default_factory=generate_name)
    dataset_name: str
    runtime_id: UUID
    user_code_id: UUID
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    user_metadata: dict = {}
    status: JobStatus = JobStatus.pending_code_review
    error: JobErrorKind = JobErrorKind.no_error
    error_message: str | None = None
    output_url: SyftBoxURL | None = None

    @property
    def user_code(self) -> "UserCode":
        client = self._client
        return client.user_code.get(self.user_code_id)

    @property
    def runtime(self) -> "Runtime":
        """Get the runtime of the job"""
        client = self._client
        return client.runtime.get(self.runtime_id)

    @property
    def runtime_name(self) -> str:
        """Get the name of the runtime of the job"""
        return self.runtime.name

    def describe(self) -> None:
        html_description = create_html_repr(
            obj=self,
            fields=[
                "uid",
                "created_by",
                "created_at",
                "updated_at",
                "name",
                "description",
                "status",
                "error",
                "error_message",
                "output_path",
                "dataset_name",
                "user_code_id",
            ],
            display_paths=["output_path"],
        )
        display(HTML(html_description))

    def show_user_code(self) -> None:
        user_code = self.user_code
        user_code.describe()

    def get_update_for_reject(self, reason: str = "unknown reason") -> "JobUpdate":
        """
        Create a JobUpdate object with the rejected status
        based on the current status
        """
        allowed_statuses = (
            JobStatus.pending_code_review,
            JobStatus.job_run_finished,
            JobStatus.job_run_failed,
        )
        if self.status not in allowed_statuses:
            raise ValueError(f"Cannot reject job in status: {self.status}")

        self.error_message = reason
        self.status = JobStatus.rejected
        self.error = (
            JobErrorKind.failed_code_review
            if self.status == JobStatus.pending_code_review
            else JobErrorKind.failed_output_review
        )
        return JobUpdate(
            uid=self.uid,
            status=self.status,
            error=self.error,
            error_message=self.error_message,
        )

    def get_update_for_in_progress(self) -> "JobUpdate":
        return JobUpdate(
            uid=self.uid,
            status=JobStatus.job_in_progress,
        )

    def get_update_for_return_code(
        self, return_code: int | subprocess.Popen, error_message: str | None = None
    ) -> "JobUpdate":
        if not isinstance(return_code, int):
            return self.get_update_for_in_progress()
        if return_code == 0:
            self.status = JobStatus.job_run_finished
            self.error = JobErrorKind.no_error
            self.error_message = None
        else:
            self.status = JobStatus.job_run_failed
            self.error = JobErrorKind.execution_failed
            self.error_message = (
                error_message
                or "Job execution failed. Please check the logs for details."
            )
        return JobUpdate(
            uid=self.uid,
            status=self.status,
            error=self.error,
            error_message=self.error_message,
        )

    @property
    def output_path(self) -> Path:
        return self.get_output_path()

    def get_output_path(self) -> Path:
        if self.output_url is None:
            raise ValueError("output_url is not set")
        client = self._client
        return self.output_url.to_local_path(
            datasites_path=client._syftbox_client.datasites
        )

    @model_validator(mode="after")
    def validate_status(self):
        if (
            self.status == JobStatus.job_run_failed
            and self.error == JobErrorKind.no_error
        ):
            raise ValueError("error must be set if status is failed")
        return self


class JobUpdate(ItemBaseUpdate[Job]):
    status: Optional[JobStatus] = None
    error: Optional[JobErrorKind] = None
    error_message: Optional[str] = None


class JobCreate(ItemBaseCreate[Job]):
    dataset_name: str
    user_code_id: UUID
    runtime_id: UUID
    name: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)


class JobConfig(BaseModel):
    """Configuration for a job run"""

    function_folder: Path
    args: list[str]
    data_path: Path
    runtime: Runtime
    job_folder: Optional[Path] = Field(
        default_factory=lambda: Path("jobs") / datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    timeout: int = 60
    data_mount_dir: str = "/app/data"
    extra_env: dict[str, str] = {}

    @property
    def job_path(self) -> Path:
        """Derived path for job folder"""
        return Path(self.job_folder)

    @property
    def logs_dir(self) -> Path:
        """Derived path for logs directory"""
        return self.job_path / "logs"

    @property
    def output_dir(self) -> Path:
        """Derived path for output directory"""
        return self.job_path / "output"

    def get_env(self) -> dict[str, str]:
        return self.extra_env | self._base_env

    def get_env_as_docker_args(self) -> list[str]:
        return [f"-e {k}={v}" for k, v in self.get_env().items()]

    def get_extra_env_as_docker_args(self) -> list[str]:
        return [f"-e {k}={v}" for k, v in self.extra_env.items()]

    @property
    def _base_env(self) -> dict[str, str]:
        interpreter = " ".join(self.runtime.cmd)
        # interpreter_str = f"'{interpreter}'" if " " in interpreter else interpreter
        return {
            "OUTPUT_DIR": str(self.output_dir.absolute()),
            "DATA_DIR": str(self.data_path.absolute()),
            "CODE_DIR": str(self.function_folder.absolute()),
            "TIMEOUT": str(self.timeout),
            "INPUT_FILE": str(self.function_folder / self.args[0]),
            "INTERPRETER": interpreter,
        }


class JobOutput(BaseModel):
    job: Job
    execution_output_dir: Path

    @property
    def logs_dir(self) -> Path:
        return self.execution_output_dir / "logs"

    @property
    def output_dir(self) -> Path:
        return self.execution_output_dir / "output"

    @property
    def stderr_file(self) -> Path:
        return self.logs_dir / "stderr.log"

    @property
    def stdout_file(self) -> Path:
        return self.logs_dir / "stdout.log"

    @property
    def stderr(self) -> str | None:
        if self.stderr_file.exists():
            return self.stderr_file.read_text()
        return None

    @property
    def stdout(self) -> str | None:
        if self.stdout_file.exists():
            return self.stdout_file.read_text()
        return None

    @property
    def log_files(self) -> list[Path]:
        return list(self.logs_dir.glob("*"))

    @property
    def output_files(self) -> list[Path]:
        return list(self.output_dir.glob("*"))

    @property
    def outputs(self) -> dict[str, Any]:
        output_files = list(self.output_dir.glob("*.json"))
        outputs = {}
        for file in output_files:
            if file.name.endswith(".json"):
                with open(file, "r") as f:
                    outputs[file.name] = json.load(f)
            elif file.name.endswith(".parquet"):
                import pandas as pd

                outputs[file.name] = pd.read_parquet(file)
            elif file.name.endswith(".csv"):
                import pandas as pd

                outputs[file.name] = pd.read_csv(file)
            elif file.name.endswith(".txt"):
                with open(file, "r") as f:
                    outputs[file.name] = f.read()
            else:
                rich.print(
                    f":warning: Unsupported file type {file.name}. Please check this file manually."
                )
        return outputs

    def describe(self):
        display_paths = ["output_dir"]
        if self.stdout_file.exists():
            display_paths.append("stdout_file")
        if self.stderr_file.exists():
            display_paths.append("stderr_file")

        html_repr = create_html_repr(
            obj=self,
            fields=["output_dir", "logs_dir"],
            display_paths=display_paths,
        )

        display(HTML(html_repr))


class JobArtifactKind(str, enum.Enum):
    computation_result = "computation_result"
    error_log = "error_log"
    execution_log = "execution_log"
