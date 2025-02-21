from pydantic import BaseModel, Field
from uuid import UUID


class CreateDataset(BaseModel):
    name: str = Field(description="Name of the dataset.")
    private_data_path: str = Field(description="Private path of the dataset.")
    mock_data_path: str = Field(description="Mock path of the dataset.")
    summary: str | None = Field(description="Summary of the dataset.")
    description_path: str | None = Field(description="Summary of the dataset.")


class Dataset(CreateDataset):
    id: UUID = Field(description="Unique identifier for the dataset.")
    created_at: str = Field(description="Timestamp when the dataset was created.")


class UpdateDataset(BaseModel):
    pass


class DatasetResponse(BaseModel):
    msg: str = Field(description="Response message.")
