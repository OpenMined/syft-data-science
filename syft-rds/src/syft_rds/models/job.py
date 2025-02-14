

from datetime import datetime
from typing import ClassVar
from uuid import UUID, uuid4
from pydantic import BaseModel, Field
from syft_rds.models.base_model import Item



class Job(Item):
    cls_name: ClassVar[str] = "SyftJob"
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    code_id: UUID
    kwargs: dict[str, UUID]
    status: str = "pending"
    result_id: UUID
    
class JobCreate(BaseModel):
    code_id: UUID
    kwargs: dict[str, UUID]
    result_id: UUID = Field(default_factory=uuid4)
    
    def to_item(self) -> Job:
        return Job(code_id=self.code_id, kwargs=self.kwargs, result_id=self.result_id)
    

