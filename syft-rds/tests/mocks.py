from syft_rds.store import BaseSpec

class MockUserSpec(BaseSpec):
    __spec_name__ = "user"

    name: str
    email: str
    tags: list[str] = []