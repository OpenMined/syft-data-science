from pydantic import Field

from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate


class Runtime(ItemBase):
    __schema_name__ = "runtime"

    name: str
    description: str
    tags: list[str] = Field(default_factory=list)


class RuntimeUpdate(ItemBaseUpdate[Runtime]):
    pass


class RuntimeCreate(ItemBaseCreate[Runtime]):
    name: str
    description: str
    tags: list[str] = Field(default_factory=list)
