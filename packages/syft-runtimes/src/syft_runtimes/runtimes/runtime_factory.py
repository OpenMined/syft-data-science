from pathlib import Path
from typing import Dict, Any, Optional, Union

from syft_core import Client as SyftBoxClient

from syft_runtimes.runtimes import Runtime, RuntimeType
from syft_runtimes.runtimes import LocalRuntime, SSHRuntime


class RuntimeFactory:
    """Factory for creating runtime instances."""

    _runtime_classes = {
        RuntimeType.LOCAL: LocalRuntime,
        RuntimeType.SSH: SSHRuntime,
        # RuntimeType.KUBERNETES: KubernetesRuntime,
        # Add other runtime types as they're implemented
    }

    @classmethod
    def create_runtime(
        self,
        runtime_type: Union[str, RuntimeType],
        runtime_name: str,
        config: Dict[str, Any],
        syftbox_client: Optional[SyftBoxClient] = None,
    ) -> Runtime:
        """Create a runtime instance based on type and configuration."""

        if isinstance(runtime_type, str):
            runtime_type = RuntimeType(runtime_type)

        if runtime_type not in self._runtime_classes:
            raise ValueError(f"Unsupported runtime type: {runtime_type}")

        runtime_class = self._runtime_classes[runtime_type]

        # Create runtime based on type
        if runtime_type == RuntimeType.LOCAL:
            if syftbox_client is None:
                raise ValueError("SyftBoxClient required for local runtime")
            return runtime_class(syftbox_client, runtime_name)

        elif runtime_type == RuntimeType.SSH:
            ssh_config = config.get("ssh", {})
            remote_base_dir = Path(config.get("remote_base_dir", "/home/user/SyftBox"))
            return runtime_class(runtime_name, ssh_config, remote_base_dir)

        elif runtime_type == RuntimeType.KUBERNETES:
            k8s_config = config.get("kubernetes", {})
            return runtime_class(runtime_name, k8s_config)

        else:
            raise NotImplementedError(
                f"Runtime creation not implemented for {runtime_type}"
            )

    @classmethod
    def register_runtime_class(cls, runtime_type: RuntimeType, runtime_class: type):
        """Register a new runtime class."""
        cls._runtime_classes[runtime_type] = runtime_class

    @classmethod
    def get_supported_runtime_types(cls) -> list[RuntimeType]:
        """Get list of supported runtime types."""
        return list(cls._runtime_classes.keys())


# Convenience functions
def create_local_runtime(
    syftbox_client: SyftBoxClient, runtime_name: str
) -> LocalRuntime:
    """Create a local runtime."""
    return RuntimeFactory.create_runtime(
        RuntimeType.LOCAL, runtime_name, {}, syftbox_client
    )


def create_ssh_runtime(
    runtime_name: str, ssh_config: Dict[str, Any], remote_base_dir: Path
) -> SSHRuntime:
    """Create an SSH runtime."""
    return RuntimeFactory.create_runtime(
        RuntimeType.SSH,
        runtime_name,
        {"ssh": ssh_config, "remote_base_dir": remote_base_dir},
    )


# def create_kubernetes_runtime(
#     runtime_name: str,
#     k8s_config: Dict[str, Any]
# ) -> KubernetesRuntime:
#     """Create a Kubernetes runtime."""
#     return RuntimeFactory.create_runtime(
#         RuntimeType.KUBERNETES,
#         runtime_name,
#         {"kubernetes": k8s_config}
#     )
