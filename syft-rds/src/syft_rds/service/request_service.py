
from typing import ClassVar, Type
from uuid import UUID
from syft_rds.db.multi_file_db import MultiFileDBEngine
from syft_rds.models.base_model import Item
from syft_rds.models.request import Request
from syft_rds.service.context import BaseRPCContext
from syft_rds.service.service import BaseService


class RequestService(BaseService):
    
    item_type: ClassVar[Type[Item]] = Request
    
    @classmethod
    def from_context(cls, context: BaseRPCContext):
        return cls(db=MultiFileDBEngine.from_context(context, item_type=Request))
    
    def approve_request(self, request_id: UUID):
        current_item = self.db.get_item(request_id)
        if current_item is None:
            raise ValueError(f"Request with id {request_id} not found")
        current_item.status = "approved"
        self.db.update_item(current_item)
