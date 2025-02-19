from syft_rds.models.base_model import ItemBase, ItemBaseCreate, ItemBaseUpdate


class Dataset(ItemBase):
    name: str
    description: str | None = None


class DatasetCreate(ItemBaseCreate[Dataset]):
    name: str
    description: str | None = None


class DatasetUpdate(ItemBaseUpdate[Dataset]):
    name: str | None = None
    description: str | None = None
