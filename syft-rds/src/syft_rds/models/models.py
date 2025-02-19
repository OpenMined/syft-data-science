from pathlib import Path
from typing import Self
from uuid import UUID

from pydantic import Field

from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate


class UserCode(ItemBase):
    name: str
    path: Path
    dataset_id: UUID

    @property
    def dataset(self) -> Dataset:
        if self.dataset_id in self._client_cache:
            return self._client_cache[self.dataset_id]
        else:
            raise Exception("Dataset not found")


class Job(ItemBase):
    name: str
    description: str
    runtime: str
    user_code_id: UUID
    tags: list[str] = Field(default_factory=list)

    @property
    def user_code(self) -> UserCode:
        if self.user_code_id in self._client_cache:
            return self._client_cache[self.user_code_id]
        else:
            raise Exception("UserCode not found")


class UserCodeCreate(ItemBaseCreate[UserCode]):
    name: str
    path: Path
    dataset_id: UUID


class JobCreate(ItemBaseCreate[Job]):
    name: str
    description: str
    runtime: str
    user_code_id: UUID
    tags: list[str] = Field(default_factory=list)

    @classmethod
    def from_code_str(cls, code: str) -> Self:
        pass

    @classmethod
    def from_func(cls, func: callable) -> Self:
        pass
