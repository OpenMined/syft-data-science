from typing import ClassVar, Type
from syft_rds.db.db import BaseQueryEngine
from syft_rds.db.multi_file_db import MultiFileDBEngine
from syft_rds.models.base_model import Item
from syft_rds.models.code import Code
from syft_rds.service.context import BaseRPCContext
from syft_rds.service.service import BaseService

class CodeService(BaseService):
    
    item_type: ClassVar[Type[Item]] = Code
    
    @classmethod
    def from_context(cls, context: BaseRPCContext):
        return cls(db=MultiFileDBEngine.from_context(context, item_type=Code))
    
    
def get_code_service() -> CodeService:
    return CodeService(db=BaseQueryEngine())