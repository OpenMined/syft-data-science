from enum import Enum
from pathlib import Path
from typing import Literal
import json
import hashlib
from typing import Any

from loguru import logger
from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
)

from syft_rds.client.utils import PathLike
from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate


class RuntimeKind(str, Enum):
    PYTHON = "python"
    DOCKER = "docker"
    KUBERNETES = "kubernetes"


class BaseRuntimeConfig(BaseModel):
    """Base configuration for runtime environments."""

    cmd: list[str] | None = None

    def validate_config(self) -> bool:
        """Override in subclasses for custom validation."""
        return True


class PythonRuntimeConfig(BaseRuntimeConfig):
    version: str | None = None
    requirements_file: PathLike | None = None
    cmd: list[str] = Field(default_factory=lambda: ["python"])

    @field_validator("requirements_file")
    def validate_requirements_file_exist(cls, value: PathLike) -> PathLike:
        if value is not None:
            requirements_file_path = Path(value).expanduser().resolve()
            if not requirements_file_path.exists():
                raise FileNotFoundError(f"Requirements file '{value}' does not exist")
        return value


class DockerMount(BaseModel):
    source: PathLike
    target: PathLike
    mode: Literal["ro", "rw"] = "ro"

    @field_validator("source")
    def validate_source_path_exists(cls, value: PathLike) -> PathLike:
        source_path = Path(value).expanduser().resolve()
        if not source_path.exists():
            # raise FileNotFoundError(f"Source path '{value}' does not exist")
            logger.warning(f"Source path '{value}' does not exist")
        return value


class DockerRuntimeConfig(BaseRuntimeConfig):
    dockerfile: PathLike | None = Field(default=None, exclude=True)
    entrypoint_script: PathLike | None = Field(default=None, exclude=True)
    dockerfile_content: str
    image_name: str | None = None
    cmd: list[str] = ["python"]
    app_name: str | None = None
    extra_mounts: list[DockerMount] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def load_content_from_path(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        If a 'dockerfile' path is provided, read its content into 'dockerfile_content'.
        This validator runs before any other field validation.
        """
        if "dockerfile" in values and values["dockerfile"] is not None:
            dockerfile_path = Path(values["dockerfile"]).expanduser().resolve()
            if not dockerfile_path.exists():
                raise FileNotFoundError(
                    f"The specified Dockerfile does not exist at: {dockerfile_path}"
                )
            values["dockerfile_content"] = dockerfile_path.read_text()
        elif "dockerfile_content" not in values:
            raise ValueError(
                "You must provide a path to a Dockerfile via the 'dockerfile' argument."
            )
        return values

    @field_validator("dockerfile_content")
    def validate_dockerfile_content(cls, dockerfile_content: str) -> str:
        if not dockerfile_content:
            raise ValueError("Dockerfile cannot be empty")
        return dockerfile_content.strip()

    def __hash__(self) -> int:
        return hash(self.dockerfile_content)

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, DockerRuntimeConfig):
            return False
        return self.dockerfile_content == __value.dockerfile_content


class KubernetesRuntimeConfig(BaseRuntimeConfig):
    image: str
    namespace: str = "syft-rds"
    num_workers: int = 1
    cmd: list[str] = Field(default_factory=list)


RuntimeConfig = PythonRuntimeConfig | DockerRuntimeConfig | KubernetesRuntimeConfig


class Runtime(ItemBase):
    __schema_name__ = "runtime"
    __table_extra_fields__ = [
        "name",
        "kind",
    ]

    name: str | None = None
    kind: RuntimeKind
    config: RuntimeConfig = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    description: str | None = Field(default=None)

    @model_validator(mode="after")
    def set_name_with_prefix(self):
        if self.name is not None:
            return self
        # Create a unique name for the runtime based on the config's hash
        # TODO: create hash based on Docker file's content
        config_dict = self.config.model_dump(mode="json")
        config_str = json.dumps(config_dict, sort_keys=True)
        config_hash = hashlib.sha256(config_str.encode()).hexdigest()[:6]
        self.name = f"{self.kind.value.lower()}_{config_hash}"
        return self

    @property
    def cmd(self) -> list[str]:
        return self.config.cmd


class RuntimeCreate(ItemBaseCreate[Runtime]):
    name: str | None = None
    kind: RuntimeKind
    config: RuntimeConfig = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    description: str | None = Field(default=None)

    @model_validator(mode="after")
    def set_name_with_prefix(self):
        if self.name is not None:
            return self
        # Create a unique name for the runtime based on the config's hash
        config_dict = self.config.model_dump(mode="json")
        config_str = json.dumps(config_dict, sort_keys=True)
        config_hash = hashlib.sha256(config_str.encode()).hexdigest()[:6]
        self.name = f"{self.kind.value.lower()}_{config_hash}"
        return self


class RuntimeUpdate(ItemBaseUpdate[Runtime]):
    pass
