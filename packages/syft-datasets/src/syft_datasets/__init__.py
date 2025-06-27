from loguru import logger
from syft_core import Client as SyftBoxClient
from syft_core.types import PathLike

_syftbox_client: SyftBoxClient | None = None


def get_syftbox_client() -> SyftBoxClient:
    """Get the global SyftBox client, initializing if needed."""
    global _syftbox_client
    if _syftbox_client is None:
        try:
            _syftbox_client = SyftBoxClient.load()
            logger.info("Logged in as {}", _syftbox_client.email)
        except Exception as e:
            logger.error("Failed to load SyftBox client: {}", e)
            raise
    return _syftbox_client


def set_syftbox_client(config_path: PathLike | None = None) -> None:
    """Set the global syftbox client for syft-datasets."""
    global _syftbox_client
    try:
        _syftbox_client = SyftBoxClient.load(filepath=config_path)
        logger.info("SyftBox client set to {}", _syftbox_client.email)
    except Exception as e:
        logger.error("Failed to set SyftBox client: {}", e)
        raise
