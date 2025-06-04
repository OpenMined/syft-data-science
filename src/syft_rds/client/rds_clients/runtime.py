from loguru import logger
import os

from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.models.models import (
    Runtime,
    RuntimeCreate,
    RuntimeKind,
    RuntimeConfig,
    PythonRuntimeConfig,
    DockerRuntimeConfig,
    KubernetesRuntimeConfig,
)

DEFAULT_RUNTIME_KIND = os.getenv("SYFT_RDS_DEFAULT_RUNTIME_KIND", RuntimeKind.PYTHON)


class RuntimeRDSClient(RDSClientModule[Runtime]):
    ITEM_TYPE = Runtime

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

    def create(
        self, runtime_kind: str | None = None, config: dict | None = None
    ) -> Runtime:
        if runtime_kind is None:
            runtime_kind = DEFAULT_RUNTIME_KIND
            logger.warning(
                f"No runtime type provided, using default runtime: `{DEFAULT_RUNTIME_KIND}`"
            )

        if runtime_kind not in [r.value for r in RuntimeKind]:
            raise ValueError(f"Invalid runtime: {runtime_kind}")

        runtime_config: RuntimeConfig = self._create_runtime_config(
            runtime_kind, config
        )
        runtime_create: RuntimeCreate = RuntimeCreate(
            kind=RuntimeKind(runtime_kind), config=runtime_config
        )
        # TODO: check if runtime_create with the same config (based on the name with hash) already exists
        runtime: Runtime = self.rpc.runtime.create(runtime_create)
        logger.info(f"Created runtime: {runtime}")

        return runtime

    def delete(self, name: str) -> None:
        return self.rpc.runtime.delete(name)
