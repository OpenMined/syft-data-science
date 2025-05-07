from pydantic import BaseModel
from typing import Literal


class PythonRuntimeConfig(BaseModel):
    type: Literal["python"] = "python"
    # Add python-specific options if needed, e.g., python_version


class DockerRuntimeConfig(BaseModel):
    type: Literal["docker"] = "docker"
    image: str | None = None
    dockerfile: PathLike | None = None
    # Add other docker options, e.g., build_args, context_path

    # Add validation if needed, e.g., ensure image or dockerfile is provided


class KubernetesRuntimeConfig(BaseModel):
    type: Literal["kubernetes"] = "kubernetes"
    image: str
    namespace: str | None = None
