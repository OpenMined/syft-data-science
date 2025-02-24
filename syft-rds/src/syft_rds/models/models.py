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
    name: str
    description: str
    tags: list[str] = Field(default_factory=list)


class DatasetUpdate(ItemBaseUpdate[Dataset]):
    pass


class GetOneRequest(BaseModel):
    uid: UUID | None = None


class GetAllRequest(BaseModel):
    pass
