from syft_rds.models.base import BaseSchema


class MockUserSchema(BaseSchema):
    __schema_name__ = "user"

    name: str
    email: str
    tags: list[str] = []
