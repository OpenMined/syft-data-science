import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Optional
from uuid import UUID

from IPython.display import HTML, display
from pydantic import Field, model_validator
from syft_core import SyftBoxURL

from syft_display_utils.html_format import create_html_repr
from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate
from syft_runtimes.models import Runtime, JobStatus, JobErrorKind
from syft_rds.utils.name_generator import generate_name

if TYPE_CHECKING:
    from syft_rds.models import CustomFunction, UserCode


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
    custom_function_id: Optional[UUID] = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    user_metadata: dict = {}
    status: JobStatus = JobStatus.pending_code_review
    error: JobErrorKind = JobErrorKind.no_error
    error_message: str | None = None
    output_url: SyftBoxURL | None = None

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
                "user_code_name",
                "custom_function_name",
            ],
            display_paths=["output_path"],
        )
        display(HTML(html_description))

    @property
    def runtime(self) -> "Runtime":
        """Get the runtime of the job"""
        client = self._client
        return client.runtime.get(self.runtime_id)

    @property
    def runtime_name(self) -> str:
        """Get the name of the runtime of the job"""
        return self.runtime.name

    @property
    def user_code(self) -> "UserCode":
        client = self._client
        return client.user_code.get(self.user_code_id)

    @property
    def user_code_name(self) -> str:
        return self.user_code.name

    @property
    def custom_function(self) -> "Optional[CustomFunction]":
        if self.custom_function_id is None:
            return None
        client = self._client
        return client.custom_function.get(self.custom_function_id)

    @property
    def custom_function_name(self) -> Optional[str]:
        if self.custom_function is None:
            return None
        return self.custom_function.name

    def show_user_code(self) -> None:
        user_code = self.user_code
        user_code.describe()

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
            self.error_message = error_message

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
    custom_function_id: Optional[UUID] = None
