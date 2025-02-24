from .store import BaseSpec
from syft_core.url import SyftBoxURL


class DatasetSpec(BaseSpec):
    __spec_name__ = "dataset"

    name: str
    description: str
    data: SyftBoxURL
    mock: SyftBoxURL
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
