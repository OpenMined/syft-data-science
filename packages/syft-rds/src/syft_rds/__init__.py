__version__ = "0.1.4"

from syft_rds.utils.paths import (
    SYFT_DATASCIENCE_NOTEBOOKS_PATH,  # noqa
    SYFT_DATASCIENCE_REPO_PATH,  # noqa
)
from syft_notebook_ui import display  # noqa
from syft_core import Client as SyftBoxClient  # noqa
from syft_rds.client.rds_client import init_session, RDSClient  # noqa
from syft_rds.client.setup import discover_rds_apps  # noqa
