import enum
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional
from uuid import UUID

import rich
from IPython.display import HTML, display
from pydantic import BaseModel, Field, model_validator
from syft_core import SyftBoxURL

from syft_rds.display_utils.html_format import create_html_repr
from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate
from syft_rds.utils.name_generator import generate_name

if TYPE_CHECKING:
    from syft_rds.models.user_code_models import UserCode


class JobStatus(str, enum.Enum):
    pending_code_review = "pending_code_review"
    job_run_failed = "job_run_failed"
    job_run_finished = "job_run_finished"

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
        "status",
        "error",
        "error_message",
    ]

    name: str = Field(default_factory=generate_name)
    description: str | None = None
    user_code_id: UUID
    tags: list[str] = Field(default_factory=list)
    user_metadata: dict = {}
    status: JobStatus = JobStatus.pending_code_review
    error: JobErrorKind = JobErrorKind.no_error
    error_message: str | None = None
    output_url: SyftBoxURL | None = None
    dataset_name: str

    @property
    def user_code(self) -> "UserCode":
        client = self._client
        return client.user_code.get(self.user_code_id)

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

    def get_update_for_return_code(self, return_code: int) -> "JobUpdate":
        if return_code == 0:
            self.status = JobStatus.job_run_finished
        else:
            self.status = JobStatus.job_run_failed
            self.error = JobErrorKind.execution_failed
            self.error_message = (
                "Job execution failed. Please check the logs for details."
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
    name: str | None = None
    description: str | None = None
    user_code_id: UUID
    tags: list[str] = Field(default_factory=list)
    dataset_name: str


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
