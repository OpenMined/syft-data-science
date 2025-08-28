import shutil
import sys
import tempfile
from pathlib import Path
from typing import Optional

from loguru import logger
from syft_core import Client as SyftBoxClient
from syft_core import SyftClientConfig

from syft_rds.client.rds_client import init_session as init_session_rds
from syft_rds.client.utils import PathLike
from syft_rds.server.app import create_app


def setup_logger(level: str = "DEBUG") -> None:
    """
    Setup loguru logger with custom filtering.

    Args:
        level (str): Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Remove default handler
    logger.remove()

    # Add custom handler that filters out noisy logs
    logger.add(
        sys.stderr,
        level=level,
        filter=lambda record: "syft_event.server2" not in record["name"],
    )


class RDSStack:
    """
    Simple wrapper for RDS stack with SyftBox clients and RDS server
    Includes 1 DO and 1 DS client
    """

    def __init__(
        self,
        do_client: SyftBoxClient,
        ds_client: SyftBoxClient,
        **config_kwargs,
    ):
        self.do_client = do_client
        self.ds_client = ds_client

        self.server = create_app(do_client)
        self.server.start()

        self.do_rds_client = init_session_rds(
            host=do_client.email,
            syftbox_client=do_client,
            **config_kwargs,
        )

        self.ds_rds_client = init_session_rds(
            host=do_client.email,
            syftbox_client=ds_client,
            **config_kwargs,
        )

    def stop(self) -> None:
        return self.server.stop()


def _prepare_root_dir(
    root_dir: Optional[PathLike] = None,
    reset: bool = False,
    key: str = "shared_client_dir",
) -> Path:
    if root_dir is None:
        root_path = Path(tempfile.gettempdir(), key)
    else:
        root_path = Path(root_dir).resolve() / key

    if reset and root_path.is_dir():
        try:
            shutil.rmtree(root_path)
        except Exception as e:
            logger.warning(f"Failed to reset directory {root_path}: {e}")

    root_path.mkdir(parents=True, exist_ok=True)
    return root_path


def remove_rds_stack_dir(
    key: str = "shared_client_dir", root_dir: Optional[PathLike] = None
) -> None:
    root_path = (
        Path(root_dir).resolve() / key if root_dir else Path(tempfile.gettempdir(), key)
    )

    if not root_path.exists():
        logger.warning(f"⚠️ Skipping removal, as path {root_path} does not exist")
        return None

    try:
        shutil.rmtree(root_path)
        logger.info(f"✅ Successfully removed directory {root_path}")
    except Exception as e:
        logger.error(f"❌ Failed to remove directory {root_path}: {e}")


def setup_rds_stack(
    root_dir: Optional[PathLike] = None,
    do_email: str = "data_owner@test.openmined.org",
    ds_email: str = "data_scientist@test.openmined.org",
    reset: bool = False,
    log_level: str = "DEBUG",
    key: str = "shared_client_dir",
    **config_kwargs,
) -> RDSStack:
    setup_logger(level=log_level)
    root_dir = _prepare_root_dir(root_dir, reset, key)

    logger.warning(
        "Using shared data directory for both clients. "
        "Any file permission checks will be skipped as both clients have access to the same files."
    )

    # We also save the config files in the root dir
    do_client_config = SyftClientConfig(
        email=do_email,
        server_url="http://localhost:8080",  # Explicit server_url for proper bootstrap
        client_url="http://localhost:5000",  # not used, just for local dev
        path=root_dir / "do_config.json",
        data_dir=root_dir,
    ).save()
    do_client = SyftBoxClient(
        do_client_config,
    )

    ds_client_config = SyftClientConfig(
        email=ds_email,
        server_url="http://localhost:8080",  # Explicit server_url for proper bootstrap
        client_url="http://localhost:5001",  # not used, just for local dev
        path=root_dir / "ds_config.json",
        data_dir=root_dir,
    ).save()
    ds_client = SyftBoxClient(ds_client_config)

    logger.info(f"Launching mock RDS stack in {root_dir}")

    return RDSStack(
        do_client=do_client,
        ds_client=ds_client,
        **config_kwargs,
    )


class SingleRDSStack:
    """A single RDS stack with a SyftBox client and a RDS server"""

    def __init__(self, client: SyftBoxClient, **config_kwargs):
        self.client = client
        self.server = create_app(client)
        self.server.start()

    def init_session(self, host, **config_kwargs):
        return init_session_rds(
            host=host,
            syftbox_client=self.client,
            **config_kwargs,
        )

    def stop(self) -> None:
        return self.server.stop()


def _get_syftbox_client(
    email: str,
    root_dir: PathLike,
) -> SyftBoxClient:
    """
    Get a SyftBox client for testing.

    Args:
        email (str): Email address of the user.
        root_dir (PathLike): Directory to store the client files.

    Returns:
        SyftBoxClient: The SyftBox client.
    """
    # We also save the config files in the root dir
    client_config = SyftClientConfig(
        email=email,
        server_url="http://localhost:8080",  # Explicit server_url for proper bootstrap
        client_url="http://localhost:5000",  # not used, just for local dev
        path=root_dir / f"{email}.config.json",
        data_dir=root_dir,
    ).save()
    return SyftBoxClient(client_config)


def setup_rds_server(
    email: str,
    root_dir: Optional[PathLike] = None,
    reset: bool = False,
    key: str = "shared_client_dir",
    log_level: str = "DEBUG",
    **config_kwargs,
):
    """
    Setup a mock RDS server for testing.

    Args:
        email (str): Email address of the user.
        root_dir (Optional[PathLike]): Directory to store the server files.
        reset (bool): Whether to reset the directory.
        key (str): Key for the directory.
        log_level (str): Log level for logging.
        **config_kwargs: Additional configuration arguments.

    Returns:
        RDSStack: The RDS stack with the server and clients.
    """
    setup_logger(level=log_level)
    root_dir = _prepare_root_dir(root_dir, reset, key)

    client = _get_syftbox_client(email=email, root_dir=root_dir)

    logger.info(f"Launching mock RDS server in {root_dir.resolve()}")

    return SingleRDSStack(
        client=client,
        **config_kwargs,
    )
