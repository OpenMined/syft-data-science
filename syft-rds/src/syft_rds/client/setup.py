from syft_core import Client as SyftBoxClient

from syft_rds.server.app import APP_INFO_FILE, APP_NAME


def discover_rds_apps(syftbox_client: SyftBoxClient | None = None) -> list[str]:
    """Return all datasites that have the RDS app installed."""
    if syftbox_client is None:
        syftbox_client = SyftBoxClient.load()

    datasites_dir = syftbox_client.workspace.datasites

    datasites = []
    for dir in datasites_dir.iterdir():
        if not dir.is_dir():
            continue
        datasite_app_dir = syftbox_client.app_data(APP_NAME, datasite=dir.name)
        if (datasite_app_dir / APP_INFO_FILE).exists():
            datasites.append(dir.name)
    return datasites
