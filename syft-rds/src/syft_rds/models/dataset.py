from typing import ClassVar
from pydantic import BaseModel, Field
from syft_rds.models.base_model import Item


class DatasetCreate(BaseModel):
    name: str = Field(description="Name of the dataset")
    description: str = Field(description="Description of the dataset")
    
    def to_item(self) -> Item:
        return Dataset(
            name=self.name,
            description=self.description,
        )
    
class Dataset(Item):
    cls_name: ClassVar[str] = "SyftDataset"
    name: str = Field(description="Name of the dataset")
    description: str = Field(description="Description of the dataset")