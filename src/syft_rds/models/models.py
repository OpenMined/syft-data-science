import base64
import enum
import os
from pathlib import Path
from typing import Any, Generic, Literal, Optional, TypeVar
from uuid import UUID

from IPython.display import HTML, display
from loguru import logger
from pydantic import (
    BaseModel,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)
from syft_core import SyftBoxURL

from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate
from syft_rds.models.html_format import create_html_repr
from syft_rds.utils.name_generator import generate_name
from syft_rds.syft_runtime.main import CodeRuntime

T = TypeVar("T", bound=ItemBase)

SYFT_RDS_DATA_DIR = "SYFT_RDS_DATA_DIR"
SYFT_RDS_OUTPUT_DIR = "SYFT_RDS_OUTPUT_DIR"
MAX_USERCODE_ZIP_SIZE = 1  # MB


class UserCodeType(enum.Enum):
    FILE = "file"
    FOLDER = "folder"


class UserCode(ItemBase):
    __schema_name__ = "usercode"
    __table_extra_fields__ = [
        "name",
    ]

    name: str
    dir_url: SyftBoxURL | None = None
    code_type: UserCodeType
    entrypoint: str

    @property
    def local_dir(self) -> Path:
        if self.dir_url is None:
            raise ValueError("dir_url is not set")
        client = self._client
        return self.dir_url.to_local_path(
            datasites_path=client._syftbox_client.datasites
        )

    def describe(self) -> None:
        html_description = create_html_repr(
            obj=self,
            fields=[
                "uid",
                "created_by",
                "created_at",
                "updated_at",
                "name",
                "local_dir",
                "code_type",
                "entrypoint",
            ],
            display_paths=["local_dir"],
        )
        display(HTML(html_description))


class UserCodeCreate(ItemBaseCreate[UserCode]):
    name: Optional[str] = None
    files_zipped: bytes | None = None
    code_type: UserCodeType
    entrypoint: str

    @field_serializer("files_zipped")
    def serialize_to_str(self, v: bytes | None) -> str | None:
        # Custom serialize for zipped binary data
        if v is None:
            return None
        return base64.b64encode(v).decode()

    @field_validator("files_zipped", mode="before")
    @classmethod
    def deserialize_from_str(cls, v):
        # Custom deserialize for zipped binary data
        if isinstance(v, str):
            return base64.b64decode(v)
        return v

    @field_validator("files_zipped", mode="after")
    @classmethod
    def validate_code_size(cls, v: bytes) -> bytes:
        zip_size_mb = len(v) / 1024 / 1024
        if zip_size_mb > MAX_USERCODE_ZIP_SIZE:
            raise ValueError(
                f"Provided files too large: {zip_size_mb:.2f}MB. Max size is {MAX_USERCODE_ZIP_SIZE}MB"
            )
        return v


class UserCodeUpdate(ItemBaseUpdate[UserCode]):
    pass


class JobErrorKind(str, enum.Enum):
    no_error = "no_error"
    timeout = "timeout"
    cancelled = "cancelled"
    execution_failed = "execution_failed"
    failed_code_review = "failed_code_review"
    failed_output_review = "failed_output_review"


class JobArtifactKind(str, enum.Enum):
    computation_result = "computation_result"
    error_log = "error_log"
    execution_log = "execution_log"


class JobStatus(str, enum.Enum):
    pending_code_review = "pending_code_review"
    job_run_failed = "job_run_failed"
    job_run_finished = "job_run_finished"

    # end states
    rejected = "rejected"  # failed to pass the review
    shared = "shared"  # shared with the user
    approved = "approved"  # approved by the reviewer


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
    enclave: str = ""

    @property
    def user_code(self) -> UserCode:
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

    def get_update_for_approve(self) -> "JobUpdate":
        """
        Create a JobUpdate object with the approved status
        based on the current status
        """
        allowed_statuses = (JobStatus.pending_code_review,)
        if self.status not in allowed_statuses:
            raise ValueError(f"Cannot reject job in status: {self.status}")

        self.status = JobStatus.approved

        return JobUpdate(
            uid=self.uid,
            status=self.status,
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


class JobCreate(ItemBaseCreate[Job]):
    name: str | None = None
    description: str | None = None
    user_code_id: UUID
    tags: list[str] = Field(default_factory=list)
    dataset_name: str
    enclave: str = ""


class JobUpdate(ItemBaseUpdate[Job]):
    status: Optional[JobStatus] = None
    error: Optional[JobErrorKind] = None
    error_message: Optional[str] = None


class Runtime(ItemBase):
    __schema_name__ = "runtime"

    name: str
    description: str
    tags: list[str] = Field(default_factory=list)


class RuntimeCreate(ItemBaseCreate[Runtime]):
    name: str
    description: str
    tags: list[str] = Field(default_factory=list)


class RuntimeUpdate(ItemBaseUpdate[Runtime]):
    pass


class Dataset(ItemBase):
    __schema_name__ = "dataset"
    __table_extra_fields__ = [
        "name",
        "summary",
    ]

    name: str = Field(description="Name of the dataset.")
    private: SyftBoxURL = Field(description="Private Syft URL of the dataset.")
    mock: SyftBoxURL = Field(description="Mock Syft URL of the dataset.")
    summary: str | None = Field(description="Summary string of the dataset.")
    readme: SyftBoxURL | None = Field(description="REAMD.md Syft URL of the dataset.")
    tags: list[str] = Field(description="Tags for the dataset.")
    runtime: CodeRuntime = Field(default_factory=CodeRuntime.default)
    auto_approval: list[str] = Field(
        default_factory=list,
        description="List of datasites whose jobs will be automatically approved.",
    )

    @property
    def mock_path(self) -> Path:
        return self.get_mock_path()

    @property
    def private_path(self) -> Path:
        return self.get_private_path()

    @property
    def readme_path(self) -> Path | None:
        return self.get_readme_path()

    def get_mock_path(self) -> Path:
        mock_path: Path = self.mock.to_local_path(
            datasites_path=self._syftbox_client.datasites
        )
        if not mock_path.exists():
            raise FileNotFoundError(f"Mock file not found at {mock_path}")
        return mock_path

    def get_private_path(self) -> Path:
        """
        Will always raise FileNotFoundError for non-admin since the
        private path will never by synced
        """
        private_path: Path = self.private.to_local_path(
            datasites_path=self._syftbox_client.datasites
        )
        if not private_path.exists():
            raise FileNotFoundError(
                f"Private data not found at {private_path}. "
                f"Probably you don't have admin permission to the dataset."
            )
        return private_path

    def get_readme_path(self) -> Path | None:
        """
        Will always raise FileNotFoundError for non-admin since the
        private path will never by synced
        """
        if not self.readme:
            return None
        readme_path: Path = self.readme.to_local_path(
            datasites_path=self._syftbox_client.datasites
        )
        if not readme_path.exists():
            return None
        return readme_path

    def get_description(self) -> str:
        # read the description .md file
        if not self.readme:
            return ""
        with open(self.get_readme_path()) as f:
            return f.read()

    def describe(self):
        field_to_include = [
            "uid",
            "created_at",
            "updated_at",
            "name",
            "readme_path",
            "mock_path",
        ]
        try:
            # Only include private path if it exists
            _ = self.private_path
            field_to_include.append("private_path")
        except FileNotFoundError:
            pass

        # Only include display paths that are not None
        display_paths = []
        if self.mock_path is not None:
            display_paths.append("mock_path")
        if self.readme_path is not None:
            display_paths.append("readme_path")

        description = create_html_repr(
            obj=self,
            fields=field_to_include,
            display_paths=display_paths,
        )

        display(HTML(description))

    def set_env(self, mock: bool = True):
        if mock:
            os.environ[SYFT_RDS_DATA_DIR] = self.get_mock_path().as_posix()
        else:
            os.environ[SYFT_RDS_DATA_DIR] = self.get_private_path().as_posix()
        logger.info(
            f"Set {SYFT_RDS_DATA_DIR} to {os.environ[SYFT_RDS_DATA_DIR]} as mock={mock}"
        )


class DatasetCreate(ItemBaseCreate[Dataset]):
    name: str = Field(description="Name of the dataset.")
    path: str = Field(description="Private path of the dataset.")
    mock_path: str = Field(description="Mock path of the dataset.")
    summary: str | None = Field(description="Summary string of the dataset.")
    description_path: str | None = Field(
        description="Path to the detailed REAMD.md of the dataset."
    )
    tags: list[str] | None = Field(description="Tags for the dataset.")
    runtime: CodeRuntime | None = Field(description="Runtime for the dataset.")
    auto_approval: list[str] = Field(
        default_factory=list,
        description="List of datasites whose jobs will be automatically approved.",
    )


class DatasetUpdate(ItemBaseUpdate[Dataset]):
    summary: Optional[str] = None
    auto_approval: Optional[list[str]] = Field(
        default_factory=list,
        description="List of datasites whose jobs will be automatically approved.",
    )


class GetOneRequest(BaseModel):
    uid: UUID | None = None
    filters: dict[str, Any] = Field(default_factory=dict)


class GetAllRequest(BaseModel):
    limit: Optional[int] = None
    offset: int = 0
    filters: dict[str, Any] = Field(default_factory=dict)
    order_by: Optional[str] = "created_at"
    sort_order: Literal["desc", "asc"] = "desc"


class ItemList(BaseModel, Generic[T]):
    # Used by get_all endpoints
    items: list[T]
