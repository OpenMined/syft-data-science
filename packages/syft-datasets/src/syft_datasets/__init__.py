from loguru import logger
from syft_core.types import PathLike
from typing_extensions import Literal

from syft_datasets.dataset import Dataset
from syft_datasets.dataset_manager import SyftDatasetManager

_global_manager: SyftDatasetManager | None = None


def login(config_path: PathLike | None = None) -> SyftDatasetManager:
    global _global_manager
    _global_manager = SyftDatasetManager.load(config_path=config_path)
    logger.info(f"Logged in as {_global_manager.syftbox_client.email}")
    return _global_manager


def get_global_manager() -> SyftDatasetManager:
    global _global_manager
    if _global_manager is None:
        try:
            _global_manager = SyftDatasetManager.load()
            logger.info(f"Auto-logged in as {_global_manager.syftbox_client.email}")
        except Exception as e:
            raise ValueError(
                "syft-datasets failed to auto-login to SyftBox. Please use `syft_datasets.login(config_path=/path/to/syftbox/config.json)`."
            ) from e
    return _global_manager


def create(
    name: str,
    mock_path: PathLike,
    private_path: PathLike,
    summary: str | None = None,
    readme_path: PathLike | None = None,
    tags: list[str] | None = None,
    syftbox_config_path: PathLike | None = None,
) -> Dataset:
    dataset_manager = SyftDatasetManager.load(config_path=syftbox_config_path)
    return dataset_manager.create(
        name=name,
        mock_path=mock_path,
        private_path=private_path,
        summary=summary,
        readme_path=readme_path,
        tags=tags,
    )


def get(
    name: str,
    datasite: str | None = None,
    syftbox_config_path: PathLike | None = None,
) -> Dataset:
    dataset_manager = SyftDatasetManager.load(config_path=syftbox_config_path)
    return dataset_manager.get(name=name, datasite=datasite)


def get_all(
    datasite: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
    order_by: str | None = None,
    sort_order: Literal["asc", "desc"] = "asc",
    syftbox_config_path: PathLike | None = None,
) -> list[Dataset]:
    dataset_manager = SyftDatasetManager.load(config_path=syftbox_config_path)
    return dataset_manager.get_all(
        datasite,
        limit,
        offset,
        order_by,
        sort_order,
    )
