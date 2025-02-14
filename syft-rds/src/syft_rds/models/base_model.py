from typing import ClassVar
from uuid import UUID, uuid4
from pydantic import BaseModel, Field

from pydantic._internal._model_construction import ModelMetaclass



class StorableItemRegistry:
    items: list[type["Item"]] = []
    
    @classmethod
    def register(cls, item: type["Item"]):
        cls.items.append(item)

class AutoExecuteMeta(ModelMetaclass):
    def __init__(cls, name, bases, class_dict):
        super().__init__(name, bases, class_dict)
        if bases:  # Ensures the base class itself doesn't trigger execution
            StorableItemRegistry.register(cls)

class Item(BaseModel, metaclass=AutoExecuteMeta):
    # TODO: make sure this is always set in the subclass
    cls_name: ClassVar[str] = "SyftItem"
    uid: UUID = Field(
        description="Unique identifier of the item", default_factory=uuid4
    )


