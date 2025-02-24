from pathlib import Path
from uuid import UUID

from pydantic import BaseModel, Field

from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate


class UserCode(ItemBase):
    name: str
    path: Path


class UserCodeCreate(ItemBaseCreate[UserCode]):
    name: str = "My UserCode"
    path: Path


class UserCodeUpdate(ItemBaseUpdate[UserCode]):
    pass


class Job(ItemBase):
    name: str
    description: str | None = None
    runtime: str
    user_code_id: UUID
    tags: list[str] = Field(default_factory=list)

    @property
    def user_code(self) -> UserCode:
        if self.user_code_id in self._client_cache:
            return self._client_cache[self.user_code_id]
        else:
            raise Exception("UserCode not found")


class JobCreate(ItemBaseCreate[Job]):
    name: str
    description: str | None = None
    runtime: str
    user_code_id: UUID
    tags: list[str] = Field(default_factory=list)


class JobUpdate(ItemBaseUpdate[Job]):
    pass


class Runtime(ItemBase):
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
    name: str
    description: str
    tags: list[str] = Field(default_factory=list)


class DatasetCreate(ItemBaseCreate[Dataset]):
    name: str = Field(description="Name of the dataset.")
    path: str = Field(description="Private path of the dataset.")
    mock_path: str = Field(description="Mock path of the dataset.")
    summary: str | None = Field(description="Summary string of the dataset.")
    description_path: str | None = Field(description="REAMD.md path of the dataset.")
    # tags: list[str] = Field(description="Tags for the dataset.")


class DatasetUpdate(ItemBaseUpdate[Dataset]):
    pass


class GetOneRequest(BaseModel):
    uid: UUID | None = None


class GetAllRequest(BaseModel):
    pass
