from syft_rds.models.base import ItemBase


class MockUserSchema(ItemBase):
    __schema_name__ = "user"

    name: str
    email: str
    tags: list[str] = []
