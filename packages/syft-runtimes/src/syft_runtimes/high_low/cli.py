import shutil
from pathlib import Path

import typer
from syft_core import Client as SyftBoxClient
from syft_core import SyftClientConfig

from syft_runtimes.high_low.rsync import (
    RsyncConfig,
    RsyncEntry,
    SSHConnection,
    SyncDirection,
    get_rsync_config_path,
)

app = typer.Typer(
    name="syft-high-low",
    help="CLI for managing high and low datasites in SyftBox",
    no_args_is_help=True,
)


def _generate_high_side_name() -> str:
    import secrets

    return f"high-side-{secrets.token_hex(4)}"


def _get_default_sync_entries(
    syftbox_client: SyftBoxClient, rsync_config: RsyncConfig
) -> list[RsyncEntry]:
    private_dir = Path("private")
    relative_highside_dir = private_dir / "job_runners" / rsync_config.high_side_name
    relative_jobs_dir = relative_highside_dir / "jobs"
    relative_outputs_dir = relative_highside_dir / "outputs"
    relative_datasets_dir = relative_highside_dir / "datasets"

    low_syftbox_dir = rsync_config.low_syftbox_dir
    high_syftbox_dir = syftbox_client.workspace.data_dir

    return [
        RsyncEntry(
            local_dir=high_syftbox_dir / relative_jobs_dir,
            remote_dir=low_syftbox_dir / relative_jobs_dir,
            direction=SyncDirection.REMOTE_TO_LOCAL,
            allow_overwrite=False,
        ),
        RsyncEntry(
            local_dir=high_syftbox_dir / relative_outputs_dir,
            remote_dir=low_syftbox_dir / relative_outputs_dir,
            direction=SyncDirection.LOCAL_TO_REMOTE,
            allow_overwrite=True,
        ),
        RsyncEntry(
            local_dir=high_syftbox_dir / relative_datasets_dir,
            remote_dir=low_syftbox_dir / relative_datasets_dir,
            direction=SyncDirection.LOCAL_TO_REMOTE,
            allow_overwrite=False,
        ),
    ]


@app.command()
def init_high_datasite(
    email: str = typer.Option(..., help="Email address for the client"),
    dir: Path | None = typer.Option(
        None,
        help="Directory for the datasite data. Defaults to ~/.syftbox/high-datasites/<email>",
    ),
    force_overwrite: bool = typer.Option(
        False, help="Overwrite existing config if present"
    ),
) -> None:
    dir = dir or Path.home() / ".syftbox" / "high-datasites" / email

    if dir.exists() and not force_overwrite:
        typer.echo(
            f"Directory {dir} already exists. Use --force-overwrite to reset your high datasite."
        )
        raise typer.Exit(code=1)

    if force_overwrite and dir.exists():
        typer.echo(f"Removing existing directory {dir}")
        shutil.rmtree(dir)

    dir.mkdir(parents=True, exist_ok=True)
    config_path = dir / "config.json"
    data_dir = dir / "SyftBox"

    syft_config = SyftClientConfig(
        email=email,
        client_url="http://testserver:5000",  # Mock placeholder, not used for high datasites
        path=config_path,
        data_dir=data_dir,
    )
    syft_config.save()

    # Ensure the datasite exists without SyftBox running
    client = SyftBoxClient(conf=syft_config)
    client.datasite_path.mkdir(parents=True, exist_ok=True)

    typer.echo(f"High datasite initialized at {data_dir}")
    typer.echo(f"Configuration saved to {config_path}")


@app.command()
def init_sync_config(
    low_syftbox_dir: Path = typer.Option(
        ..., help="Path to the low datasite SyftBox directory"
    ),
    ssh_host: str = typer.Option(None, help="SSH hostname (if using SSH)"),
    ssh_user: str = typer.Option(None, help="SSH username"),
    ssh_port: int = typer.Option(22, help="SSH port"),
    ssh_key_path: Path = typer.Option(None, help="Path to SSH private key"),
    force_overwrite: bool = typer.Option(False, help="Overwrite existing sync config"),
    high_side_name: str | None = typer.Option(
        None,
        help="Name of the high datasite, required for syncing between high and low datasites. If not provided, a random name will be generated.",
    ),
    add_default_entries: bool = typer.Option(
        True, help="Add default sync entries for common directories"
    ),
    syftbox_config_path: Path | None = typer.Option(
        None, help="Path to SyftBox config file"
    ),
) -> None:
    """Initialize sync configuration for a high datasite."""
    high_side_name = high_side_name or _generate_high_side_name()
    syftbox_client = SyftBoxClient.load(filepath=syftbox_config_path)
    sync_config_path = get_rsync_config_path(syftbox_client)

    low_syftbox_dir = low_syftbox_dir.resolve()
    high_syftbox_dir = syftbox_client.workspace.data_dir

    if sync_config_path.exists() and not force_overwrite:
        typer.echo(
            f"Sync config already exists at {sync_config_path}. Use --force-overwrite to replace."
        )
        raise typer.Exit(code=1)

    # Build connection settings
    connection_settings = None
    if ssh_host:
        if not ssh_user:
            typer.echo("SSH user is required when using SSH")
            raise typer.Exit(code=1)
        connection_settings = SSHConnection(
            host=ssh_host, user=ssh_user, port=ssh_port, ssh_key_path=ssh_key_path
        )

    rsync_config = RsyncConfig(
        high_side_name=high_side_name,
        low_syftbox_dir=low_syftbox_dir,
        high_syftbox_dir=high_syftbox_dir,
        connection_settings=connection_settings,
        entries=[],
    )
    if add_default_entries:
        default_entries = _get_default_sync_entries(syftbox_client, rsync_config)
        rsync_config.entries.extend(default_entries)
    rsync_config.save(syftbox_client)

    typer.echo(f"Sync config initialized at {sync_config_path}")

    if len(rsync_config.entries) > 0:
        typer.echo("\n\nRsync commands for the configured entries:\n\n")
        for entry in rsync_config.entries:
            direction_desc = (
                f"# sync /{entry.local_dir.name} (sync {entry.direction.value})"
            )
            command = entry.to_command(connection_settings)
            typer.echo(f"{direction_desc}\n{command}\n")


@app.command()
def add_sync_entry(
    local_dir: Path = typer.Option(..., help="Local directory path"),
    remote_dir: Path = typer.Option(
        ..., help="Remote directory path (relative to remote datasite)"
    ),
    direction: SyncDirection = typer.Option(
        SyncDirection.LOCAL_TO_REMOTE,
        help="Direction of sync: 'local_to_remote' or 'remote_to_local'",
    ),
    allow_overwrite: bool = typer.Option(
        False, help="Allow overwriting existing files"
    ),
    syftbox_config_path: Path | None = typer.Option(
        None, help="Path to SyftBox config file"
    ),
) -> None:
    """Add a sync entry to the configuration."""
    syftbox_client = SyftBoxClient.load(filepath=syftbox_config_path)
    rsync_config = RsyncConfig.load(syftbox_client)

    entry = RsyncEntry(
        local_dir=local_dir,
        remote_dir=remote_dir,
        direction=direction,
        allow_overwrite=allow_overwrite,
    )
    rsync_config.entries.append(entry)
    rsync_config.save(syftbox_client)

    typer.echo(
        f"Added sync entry: {local_dir.name} <-> {remote_dir.name} ({direction})"
    )


if __name__ == "__main__":
    app()
