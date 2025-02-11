from typing import ClassVar, List
from pydantic import BaseModel, Field
from syft_rds.db.db import BaseQueryEngine
from syft_rds.service.context import BaseRPCContext
from syft_rds.models.base_model import Item


class BaseService(BaseModel):
    db: BaseQueryEngine
    item_type: ClassVar[type[Item]]
    
    
    def create_item(self, x: BaseModel) -> Item:
        item = x.to_item()
        return self.db.create_item(item)
    
    
    def list_items(self) -> List[Item]:
        return self.db.list_items()
    
    @classmethod
    def from_context(cls, context: BaseRPCContext):
        return cls(db=BaseQueryEngine.from_context(context, item_type=cls.item_type))
    
    