from uuid import UUID, uuid4
from pydantic import BaseModel, Field


class Item(BaseModel):
    cls_name: str = Field(description="Name of the class")
    uid: UUID = Field(
        description="Unique identifier of the item", default_factory=uuid4
    )

    @property
    def cls_name(self) -> str:
        return self.__class__.__name__
