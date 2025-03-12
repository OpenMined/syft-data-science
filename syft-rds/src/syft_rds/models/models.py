import base64
import enum
import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Generic, Literal, Optional, TypeVar
from uuid import UUID

from loguru import logger
from pydantic import (
    BaseModel,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)
from syft_core import SyftBoxURL

from syft_rds.models.base import BaseSchema, BaseSchemaCreate, BaseSchemaUpdate
from syft_rds.utils.name_generator import generate_name

T = TypeVar("T", bound=BaseSchema)

SYFT_RDS_DATA_DIR = "SYFT_RDS_DATA_DIR"
SYFT_RDS_OUTPUT_DIR = "SYFT_RDS_OUTPUT_DIR"
MAX_USERCODE_ZIP_SIZE = 1  # MB


class UserCode(BaseSchema):
    __schema_name__ = "usercode"

    name: str
    dir_url: SyftBoxURL | None = None
    file_name: str

    @property
    def local_dir(self) -> Path:
        if self.dir_url is None:
            raise ValueError("dir_url is not set")
        client = self._client
        return self.dir_url.to_local_path(
            datasites_path=client._syftbox_client.datasites
        )

    @property
    def local_file(self) -> Path:
        return self.local_dir / self.file_name


class UserCodeCreate(BaseSchemaCreate[UserCode]):
    name: Optional[str] = None
    files_zipped: bytes | None = None
    # TODO add support for multiple files
    file_name: str

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


class UserCodeUpdate(BaseSchemaUpdate[UserCode]):
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


class Job(BaseSchema):
    __schema_name__ = "job"
    __table_extra_fields__ = [
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
    def user_code(self) -> UserCode:
        client = self._client
        return client.user_code.get(self.user_code_id)

    class Config:
        extra = "forbid"

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

    def get_output_path(self) -> Path:
        if self.output_url is None:
            raise ValueError("output_url is not set")
        client = self._client
        return self.output_url.to_local_path(
            datasites_path=client._syftbox_client.datasites
        )

    def share_artifacts(
        self,
        include: Iterable[JobArtifactKind] = (
            JobArtifactKind.computation_result,
            JobArtifactKind.error_log,
            JobArtifactKind.execution_log,
        ),
    ):
        if self.status not in (
            JobStatus.job_run_finished,
            JobStatus.job_run_failed,
        ):
            raise ValueError(
                f"Job must be executed first. Current status: {self.status.value}"
            )
        self.status = JobStatus.shared

    @model_validator(mode="after")
    def validate_status(self):
        if (
            self.status == JobStatus.job_run_failed
            and self.error == JobErrorKind.no_error
        ):
            raise ValueError("error must be set if status is failed")
        return self


class JobCreate(BaseSchemaCreate[Job]):
    name: str | None = None
    description: str | None = None
    user_code_id: UUID
    tags: list[str] = Field(default_factory=list)
    dataset_name: str


class JobUpdate(BaseSchemaUpdate[Job]):
    status: Optional[JobStatus] = None
    error: Optional[JobErrorKind] = None
    error_message: Optional[str] = None


class Runtime(BaseSchema):
    __schema_name__ = "runtime"

    name: str
    description: str
    tags: list[str] = Field(default_factory=list)


class RuntimeCreate(BaseSchemaCreate[Runtime]):
    name: str
    description: str
    tags: list[str] = Field(default_factory=list)


class RuntimeUpdate(BaseSchemaUpdate[Runtime]):
    pass


class Dataset(BaseSchema):
    __schema_name__ = "dataset"

    name: str = Field(description="Name of the dataset.")
    private: SyftBoxURL = Field(description="Private Syft URL of the dataset.")
    mock: SyftBoxURL = Field(description="Mock Syft URL of the dataset.")
    summary: str | None = Field(description="Summary string of the dataset.")
    readme: SyftBoxURL | None = Field(description="REAMD.md Syft URL of the dataset.")
    tags: list[str] = Field(description="Tags for the dataset.")

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

    def get_readme_path(self) -> Path:
        """
        Will always raise FileNotFoundError for non-admin since the
        private path will never by synced
        """
        readme_path: Path = self.readme.to_local_path(
            datasites_path=self._syftbox_client.datasites
        )
        if not readme_path.exists():
            raise FileNotFoundError(f"Readme file not found at {readme_path}")
        return readme_path

    def get_description(self) -> str:
        # read the description .md file
        with open(self.get_readme_path()) as f:
            return f.read()

    def describe(self) -> bool:
        # prefix components:
        space = "    "
        branch = "│   "
        # pointers:
        tee = "├── "
        last = "└── "

        def tree(dir_path: Path, prefix: str = ""):
            """A recursive generator, given a directory Path object
            will yield a visual tree structure line by line
            with each line prefixed by the same characters

            Ref: https://stackoverflow.com/questions/9727673/list-directory-tree-structure-in-python
            """
            contents = list(dir_path.iterdir())
            # contents each get pointers that are ├── with a final └── :
            pointers = [tee] * (len(contents) - 1) + [last]
            for pointer, path in zip(pointers, contents):
                yield prefix + pointer + path.name
                if path.is_dir():  # extend the prefix and recurse:
                    extension = branch if pointer == tee else space
                    # i.e. space because last, └── , above so no more |
                    yield from tree(path, prefix=prefix + extension)

        try:
            for line in tree(self.get_mock_path()):
                print(line)
            return True
        except Exception as e:
            print(f"Could not display dataset structure with error: {str(e)}")
            return False

    def set_env(self, mock: bool = True):
        if mock:
            os.environ[SYFT_RDS_DATA_DIR] = self.get_mock_path().as_posix()
        else:
            os.environ[SYFT_RDS_DATA_DIR] = self.get_private_path().as_posix()
        logger.info(
            f"Set {SYFT_RDS_DATA_DIR} to {os.environ[SYFT_RDS_DATA_DIR]} as mock={mock}"
        )


class DatasetCreate(BaseSchemaCreate[Dataset]):
    name: str = Field(description="Name of the dataset.")
    path: str = Field(description="Private path of the dataset.")
    mock_path: str = Field(description="Mock path of the dataset.")
    summary: str | None = Field(description="Summary string of the dataset.")
    description_path: str | None = Field(
        description="Path to the detailed REAMD.md of the dataset."
    )
    tags: list[str] | None = Field(description="Tags for the dataset.")


class DatasetUpdate(BaseSchemaUpdate[Dataset]):
    pass


class GetOneRequest(BaseModel):
    uid: UUID | None = None
    filters: dict[str, Any] = {}


class GetAllRequest(BaseModel):
    limit: Optional[int] = None
    offset: int = 0
    filters: dict[str, Any] = {}
    order_by: Optional[str] = "created_at"
    sort_order: Literal["desc", "asc"] = "desc"


class ItemList(BaseModel, Generic[T]):
    # Used by get_all endpoints
    items: list[T]
