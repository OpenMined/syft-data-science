from typing import ClassVar
from pydantic import BaseModel, Field
from syft_rds.models.base_model import Item


class CodeCreate(BaseModel):
    code_str: str = Field(description="Name of the dataset")
    
    def to_item(self) -> Item:
        return Code(
            code_str=self.code_str,
        )
    
class Code(Item):
    cls_name: ClassVar[str] = "SyftCode"
    code_str: str = Field(description="Name of the dataset")