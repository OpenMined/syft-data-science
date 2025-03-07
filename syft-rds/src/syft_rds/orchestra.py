import shutil
import sys
import tempfile
from pathlib import Path
from typing import Optional

from loguru import logger
from syft_core import Client as SyftBoxClient
from syft_core import SyftClientConfig

from syft_rds.client.rds_client import init_session
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
    """Simple wrapper for RDS stack with SyftBox clients and RDS server"""

    def __init__(
        self,
        do_client: SyftBoxClient,
        ds_client: SyftBoxClient,
    ):
        self.do_client = do_client
        self.ds_client = ds_client

        self.server = create_app(do_client)
        self.server.start()

        self.do_rds_client = init_session(
            host=do_client.email, syftbox_client=do_client
        )

        self.ds_rds_client = init_session(
            host=do_client.email, syftbox_client=ds_client
        )

    def stop(self) -> None:
        return self.server.stop()


def _prepare_root_dir(root_dir: Optional[PathLike] = None, reset: bool = False) -> Path:
    if root_dir is None:
        return Path(tempfile.mkdtemp(prefix="rds_"))

    root_path = Path(root_dir)

    if reset and root_path.exists():
        try:
            shutil.rmtree(root_path)
        except Exception as e:
            logger.warning(f"Failed to reset directory {root_path}: {e}")

    root_path.mkdir(parents=True, exist_ok=True)
    return root_path


def setup_rds_stack(
    root_dir: Optional[PathLike] = None,
    do_email: str = "data_owner@test.openmined.org",
    ds_email: str = "data_scientist@test.openmined.org",
    reset: bool = False,
    log_level: str = "DEBUG",
) -> RDSStack:
    setup_logger(level=log_level)
    root_dir = _prepare_root_dir(root_dir, reset)

    shared_client_dir = root_dir / "shared_client_dir"
    shared_client_dir.mkdir(exist_ok=True)

    logger.warning(
        "Using shared data directory for both clients. "
        "Any file permission checks will be skipped as both clients have access to the same files."
    )

    do_client = SyftBoxClient(
        SyftClientConfig(
            email=do_email,
            client_url="http://localhost:5000",  # not used, just for local dev
            path=root_dir / "do_config.json",
            data_dir=shared_client_dir,
        ),
    )

    ds_client = SyftBoxClient(
        SyftClientConfig(
            email=ds_email,
            client_url="http://localhost:5001",  # not used, just for local dev
            path=root_dir / "ds_config.json",
            data_dir=shared_client_dir,
        ),
    )

    logger.info(f"Launching mock RDS stack in {root_dir}...")

    return RDSStack(
        do_client=do_client,
        ds_client=ds_client,
    )
