from typing import Dict, List
from syft_rds.models import DockerMount, JobConfig


class MountProvider:
    """Base class for mount providers"""

    def get_mounts(self, job_config: JobConfig) -> List[DockerMount]:
        """Get additional mounts for a job"""
        return []


# Registry to store mount providers
_mount_providers: Dict[str, MountProvider] = {}


def register_mount_provider(app_name: str, provider: MountProvider) -> None:
    """Register a mount provider for an app"""
    _mount_providers[app_name] = provider


def get_mount_provider(app_name: str) -> MountProvider | None:
    """Get a mount provider for an app"""
    return _mount_providers.get(app_name)
