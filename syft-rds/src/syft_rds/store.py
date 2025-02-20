from uuid import uuid4
from typing import Optional
from syft_core import Client
from syft_core.url import SyftBoxURL
from pydantic import BaseModel
from pydantic import EmailStr
from pydantic import Field
from enum import StrEnum
import yaml
import json


PERMS = """
- path: '**'
  permissions:
  - admin
  - read
  - write
  user: '*'
"""


class RuntimeType(StrEnum):
    docker = "docker"
    # k8s = "k8s"
    # local = "local"


class BaseSpec(BaseModel):
    __name__: str
    __indexes__: list[str]
    _id: str = Field(default_factory=uuid4)


class DatasetSpec(BaseSpec):
    __name__ = "dataset"
    __indexes__ = ["name"]

    name: str
    description: str
    data: SyftBoxURL
    mock: SyftBoxURL
    tags: list[str]


# class CodeSpec(Base):
#     language: str
#     path: SyftBoxURL
#     created_by: EmailStr


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


class RDSStore:
    specs: list = [DatasetSpec]  # , CodeSpec, RuntimeSpec, JobSpec]

    def __init__(
        self,
        app_name: str,
        client: Optional[Client] = None,
    ):
        self.app_name = app_name
        if client is None:
            self.client = Client.load()
        self.app_dir = self.client.api_data(self.app_name)
        self.app_store_dir = self.app_dir / "store"

        # Create app store directory
        self.app_store_dir.mkdir(exist_ok=True, parents=True)

        # Create restrictive Permissions for the store Directory
        perms = self.app_store_dir / "syftperm.yaml"
        perms.write_text(PERMS)

        # index file
        for spec in self.specs:
            index_file_path = self.app_store_dir / f"{spec.__name__}.json"
            index_file_path.write_text("\{\}")

    @property
    def store_dir(self):
        return self.app_store_dir

    def _update_index(self, spec: BaseSpec):
        index_file_path = self.app_store_dir / f"{spec.__name__}.json"
        index = json.loads(index_file_path.read_text())
        spec_indexes = spec.__indexes__
        for idx in spec_indexes:
            key = getattr(spec, idx)
            if key not in index:
                index[key] = set()
            index[key].add(spec._id)
        index_file_path.write_text(json.dumps(index))

    def create(self, spec: BaseSpec) -> str:
        file_path = self.app_store_dir / spec.__name__ / f"{spec._id}.yaml"
        json_dump = spec.model_dump_json()
        yaml_dump = yaml.dump(json_dump)
        file_path.write_text(yaml_dump)
        self._update_index(spec)
        return spec._id
