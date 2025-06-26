from loguru import logger
import os
from pathlib import Path

from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.models import (
    Runtime,
    RuntimeCreate,
    RuntimeKind,
    RuntimeConfig,
    PythonRuntimeConfig,
    DockerRuntimeConfig,
    KubernetesRuntimeConfig,
)

DEFAULT_RUNTIME_KIND = os.getenv("SYFT_RDS_DEFAULT_RUNTIME_KIND", "python")
DEFAULT_RUNTIME_NAME = os.getenv(
    "SYFT_RDS_DEFAULT_RUNTIME_NAME", "syft_default_python_runtime"
)
PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_DOCKERFILE_FILE_PATH = PROJECT_ROOT / "runtimes" / "python.Dockerfile"


class RuntimeRDSClient(RDSClientModule[Runtime]):
    ITEM_TYPE = Runtime

    def create(
        self,
        runtime_name: str | None = None,
        runtime_kind: str | None = None,
        config: dict | None = None,
        description: str | None = None,
    ) -> Runtime:
        """
        Create a runtime.
        """
        if runtime_name is None and runtime_kind is None:
            return self._get_or_create_default()

        self._validate_runtime_args(runtime_name, runtime_kind, config)

        # TODO: Uncomment this
        # if self.get_runtime_by_name(runtime_name):
        #     raise ValueError(f"Runtime with name '{runtime_name}' already exists.")

        runtime_config: RuntimeConfig = self._create_runtime_config(
            runtime_kind, config
        )
        runtime_create: RuntimeCreate = RuntimeCreate(
            name=runtime_name,
            kind=RuntimeKind(runtime_kind),
            config=runtime_config,
            description=description,
        )

        return self.get_or_create(runtime_create)

    def get_runtime_by_name(self, name: str) -> Runtime | None:
        try:
            return self.get(name=name)
        except Exception as e:
            logger.debug(f"Error getting runtime by name: {e}")
            return None

    def get_or_create(self, runtime_create: RuntimeCreate) -> Runtime:
        fetched_runtime = self.get_runtime_by_name(runtime_create.name)

        if fetched_runtime:
            logger.debug(
                f"Runtime '{fetched_runtime.name}' already exists. Returning existing runtime."
            )
            return fetched_runtime

        runtime = self.rpc.runtime.create(runtime_create)
        logger.debug(f"Runtime created: {runtime}")

        return runtime

    def _create_runtime_config(
        self, runtime_kind: str, config: dict | None = None
    ) -> RuntimeConfig:
        if config is None:
            config = {}

        kind_str = runtime_kind.lower()

        config_map = {
            RuntimeKind.PYTHON.value: PythonRuntimeConfig,
            RuntimeKind.DOCKER.value: DockerRuntimeConfig,
            RuntimeKind.KUBERNETES.value: KubernetesRuntimeConfig,
        }

        config_class = config_map.get(kind_str)

        if config_class:
            return config_class(**config)
        else:
            raise ValueError(f"Unsupported runtime type: {runtime_kind}")

    def _get_or_create_default(self) -> Runtime:
        """
        Get the default runtime if it exists, otherwise create it.
        The default runtime is a python runtime with the python 3.12.
        """
        try:
            default_runtime_create = RuntimeCreate(
                kind=DEFAULT_RUNTIME_KIND,
                name=DEFAULT_RUNTIME_NAME,
                config=PythonRuntimeConfig(
                    version="3.12",
                ),
                description="Default Python runtime for Syft RDS",
            )
            return self.get_or_create(default_runtime_create)
        except Exception as e:
            logger.error(f"Error getting or creating default runtime: {e}")
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
