from loguru import logger
import os
from pathlib import Path

from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.models.models import (
    Runtime,
    RuntimeCreate,
    RuntimeKind,
    RuntimeConfig,
    PythonRuntimeConfig,
    DockerRuntimeConfig,
    KubernetesRuntimeConfig,
    GetOneRequest,
)

DEFAULT_RUNTIME_NAME = os.getenv("SYFT_RDS_DEFAULT_RUNTIME_NAME", "docker_python_3.12")
PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_DOCKERFILE_FILE_PATH = PROJECT_ROOT / "runtimes" / "python.Dockerfile"


class RuntimeRDSClient(RDSClientModule[Runtime]):
    ITEM_TYPE = Runtime

    def create(
        self,
        runtime_name: str | None = None,
        runtime_kind: str | None = None,
        config: dict | None = None,
    ) -> Runtime:
        """
        Create a runtime.
        """
        if runtime_name is None and runtime_kind is None:
            return self._get_or_create_default()

        self._validate_runtime_args(runtime_name, runtime_kind, config)

        runtime_config: RuntimeConfig = self._create_runtime_config(
            runtime_kind, config
        )
        runtime_create: RuntimeCreate = RuntimeCreate(
            name=runtime_name, kind=RuntimeKind(runtime_kind), config=runtime_config
        )

        fetched_runtime = self.get_runtime_by_name(runtime_create.name)
        if fetched_runtime is not None:
            logger.info(f"Runtime already exists: {fetched_runtime}")
            return fetched_runtime

        logger.info(f"Creating runtime: {runtime_name}")
        return self.rpc.runtime.create(runtime_create)

    def get_runtime_by_name(self, name: str) -> Runtime | None:
        try:
            get_req = GetOneRequest(filters={"name": name})
            return self.rpc.runtime.get_one(get_req)
        except Exception as e:
            logger.error(f"Error getting runtime by name: {e}")
            return None

    def delete(self, uid: str) -> None:
        return self.rpc.runtime.delete(uid)

    def _create_runtime_config(
        self, runtime_kind: str, config: dict | None = None
    ) -> RuntimeConfig:
        if config is None:
            config = {}
        runtime_kind = runtime_kind.lower()

        if runtime_kind == RuntimeKind.PYTHON:
            return PythonRuntimeConfig(**config)
        elif runtime_kind == RuntimeKind.DOCKER:
            return DockerRuntimeConfig(**config)
        elif runtime_kind == RuntimeKind.KUBERNETES:
            return KubernetesRuntimeConfig(**config)
        else:
            raise ValueError(f"Unsupported runtime type: {runtime_kind}")

    def _get_or_create_default(self) -> Runtime:
        """
        Get the default runtime if it exists, otherwise create it.
        The default runtime is a docker runtime with the python 3.12 image.
        The name of the default runtime is "docker_python_3.12".
        """
        try:
            default_runtime = self.get_runtime_by_name(DEFAULT_RUNTIME_NAME)
            if default_runtime is not None:
                return default_runtime
            else:
                default_runtime_create: RuntimeCreate = RuntimeCreate(
                    name=DEFAULT_RUNTIME_NAME,
                    kind=RuntimeKind.DOCKER,
                    config={
                        "dockerfile": str(DEFAULT_DOCKERFILE_FILE_PATH),
                    },
                )
                default_runtime = self.rpc.runtime.create(default_runtime_create)
                logger.info(f"Created default runtime: {default_runtime.name}")
                return default_runtime
        except Exception as e:
            logger.error(f"Error getting default runtime: {e}")
            raise e

    def _validate_runtime_args(self, runtime_name, runtime_kind, config):
        if runtime_name is not None and runtime_kind is None:
            raise ValueError(
                "Runtime kind must be provided if runtime name is provided"
            )
        if runtime_kind is not None and runtime_kind not in [
            r.value for r in RuntimeKind
        ]:
            raise ValueError(
                f"Invalid runtime kind: {runtime_kind}. Must be one of {RuntimeKind}"
            )
        if config is not None and runtime_kind is None:
            raise ValueError("Runtime kind must be provided if config is provided")
