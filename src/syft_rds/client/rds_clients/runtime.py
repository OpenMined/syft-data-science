from loguru import logger

from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.models.models import (
    Runtime,
    RuntimeCreate,
    RuntimeType,
    BaseRuntimeConfig,
    PythonRuntimeConfig,
    DockerRuntimeConfig,
    KubernetesRuntimeConfig,
)


class RuntimeRDSClient(RDSClientModule[Runtime]):
    ITEM_TYPE = Runtime

    def _create_runtime_config(
        self, runtime: str, config: dict | None = None
    ) -> BaseRuntimeConfig:
        if runtime == RuntimeType.PYTHON:
            return PythonRuntimeConfig(**config)
        elif runtime == RuntimeType.DOCKER:
            return DockerRuntimeConfig(**config)
        elif runtime == RuntimeType.KUBERNETES:
            return KubernetesRuntimeConfig(**config)
        else:
            raise ValueError(f"Unsupported runtime type: {runtime}")

    def create(self, runtime: str, config: dict | None = None) -> Runtime:
        if runtime not in [r.value for r in RuntimeType]:
            raise ValueError(f"Invalid runtime: {runtime}")

        runtime_create: RuntimeCreate = RuntimeCreate(
            type=RuntimeType(runtime), **config
        )
        logger.info(f"Creating runtime: {runtime_create}")

        # return self.rpc.runtime.create(runtime_create)
        # return Runtime(uid=uuid.uuid4(), **runtime_create.model_dump())

    def delete(self, name: str) -> None:
        return self.rpc.runtime.delete(name)
