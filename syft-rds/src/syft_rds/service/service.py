from pydantic import BaseModel, Field
from syft_rds.db.db import BaseDatabase
from syft_rds.service.context import BaseRPCContext
from syft_rds.models.base_model import Item


class BaseService(BaseModel):
    db: BaseDatabase
    
    
    def create_item(self, x: BaseModel) -> Item:
        item = x.to_item()
        return self.db.create_item(item)
    
    @classmethod
    def from_context(cls, context: BaseRPCContext):
        return cls(db=BaseDatabase.from_context(context))
    
    