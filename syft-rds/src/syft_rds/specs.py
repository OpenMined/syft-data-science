from .store import BaseSpec
from syft_core.url import SyftBoxURL
from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema
from typing import Annotated, Any, Type


# Create a custom type adapter for SyftBoxURL
class SyftBoxURLType:
    """A type adapter for SyftBoxURL that can be parsed from a string."""

    @classmethod
    def __get_pydantic_core_schema__(
        cls, _source_type: Type[Any], _handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """Define how to validate and parse SyftBoxURL."""
        return core_schema.union_schema(
            [
                # Try to use the object as-is if it's already a SyftBoxURL
                core_schema.is_instance_schema(SyftBoxURL),
                # Otherwise, parse it from a string
                core_schema.chain_schema(
                    [
                        core_schema.str_schema(),
                        core_schema.no_info_plain_validator_function(
                            lambda s: SyftBoxURL(s)
                        ),
                    ]
                ),
            ]
        )


# Type alias for use in models
SyftBoxURLField = Annotated[SyftBoxURL, SyftBoxURLType]


class DatasetSpec(BaseSpec):
    __spec_name__ = "dataset"

    name: str
    description: str
    data: SyftBoxURLField
    mock: SyftBoxURLField
    tags: list[str]


# class CodeSpec(Base):
#     language: str
#     path: SyftBoxURL
#     created_by: EmailStr


# class RuntimeType(StrEnum):
#     docker = "docker"
#     # k8s = "k8s"
#     # local = "local"


# class RuntimeSpec(Base):
#     name: str
#     type: RuntimeType
#     kwargs: dict


# class JobSpec(Base):
#     code: str
#     code_hash: str
#     dataset: str
#     dataset_hash: str
#     runtime: str
#     approval_status: str
#     reason: str
#     created_by: str
#     approved_by: str
#     created_at: str
#     approved_at: str
#     tags: str
#     result_path: str
#     result_hash: str
#     logs: str
#     environment: str
