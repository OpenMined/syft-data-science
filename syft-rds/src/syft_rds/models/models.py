import enum
from collections.abc import Iterable
from pathlib import Path
from typing import Generic, Optional, TypeVar
from uuid import UUID

from pydantic import BaseModel, Field, model_validator
from syft_core import SyftBoxURL

from syft_rds.models.base import BaseSchema, BaseSchemaCreate, BaseSchemaUpdate
from syft_rds.utils.name_generator import generate_name

T = TypeVar("T", bound=BaseSchema)


class UserCode(BaseSchema):
    __schema_name__ = "usercode"

    name: str
    path: Path


class UserCodeCreate(BaseSchemaCreate[UserCode]):
    name: str = "My UserCode"
    path: Path


class UserCodeUpdate(BaseSchemaUpdate[UserCode]):
    pass


class JobErrorKind(str, enum.Enum):
    no_error = "no_error"
    timeout = "timeout"
    cancelled = "cancelled"
    error = "error"
    code_review_rejected = "code_review_rejected"
    output_review_rejected = "output_review_rejected"
    failed_review = "failed_review"


class JobArtifactKind(str, enum.Enum):
    computation_result = "computation_result"
    error_log = "error_log"
    execution_log = "execution_log"


class JobStatus(str, enum.Enum):
    pending_code_review = "pending_code_review"
    queued = "queued"
    running = "running"
    job_run_failed = "job_run_failed"
    job_run_finished = "job_run_finished"

    # end states
    rejected = "rejected"  # failed to pass the review
    shared = "shared"  # shared with the user


class Job(BaseSchema):
    __schema_name__ = "job"

    name: str = Field(default_factory=generate_name)
    description: str | None = None
    user_code_id: UUID
    tags: list[str] = Field(default_factory=list)
    user_metadata: dict = {}
    status: JobStatus = JobStatus.pending_code_review
    error: JobErrorKind = JobErrorKind.no_error
    output_url: str | None = None
    dataset_name: str

    @property
    def user_code(self) -> UserCode:
        if self.user_code_id in self._client_cache:
            return self._client_cache[self.user_code_id]
        else:
            raise Exception("UserCode not found")

    class Config:
        extra = "forbid"

    def add_to_queue(self):
        if self.status != JobStatus.pending_code_review:
            raise ValueError(
                "job must be pending code review - call submit_for_code_review() first"
            )
        self.status = JobStatus.queued

    def reject(self, reason: str = "unknown reason"):
        # artifacts are not shared on rejection
        self.status = JobStatus.rejected
        self.error = JobErrorKind.failed_review

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
    name: str = Field(default_factory=generate_name)
    description: str | None = None
    user_code_id: UUID
    tags: list[str] = Field(default_factory=list)
    dataset_name: str


class JobUpdate(BaseSchemaUpdate[Job]):
    status: Optional[JobStatus] = None
    error: Optional[JobErrorKind] = None


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
            raise FileNotFoundError(f"Private data not found at {private_path}")
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


class GetAllRequest(BaseModel):
    pass


class ItemList(BaseModel, Generic[T]):
    # Used by get_all endpoints
    items: list[T]
