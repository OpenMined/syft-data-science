from pydantic import BaseModel, Field
from uuid import UUID


class JobCreate(BaseModel):
    name: str
    description: str | None = None
    runtime: str
    user_code_id: UUID
    tags: list[str] = Field(default_factory=list)
