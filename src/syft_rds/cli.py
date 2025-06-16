from pathlib import Path
from typing import Optional

import typer
from loguru import logger
from syft_core import Client as SyftBoxClient
from syft_core import SyftClientConfig

from syft_rds.server.app import create_app

app = typer.Typer(
    name="syft-rds",
    help="Syft RDS CLI",
    no_args_is_help=True,
    pretty_exceptions_enable=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)


@app.command()
def server(
    syftbox_config: Optional[Path] = typer.Option(None),
) -> None:
    """Start the RDS server."""
    syftbox_client = SyftBoxClient.load(filepath=syftbox_config)
    typer.echo(f"SyftBox client loaded from {syftbox_client.config_path}")
    rds_app = create_app(client=syftbox_client)
    rds_app.run_forever()


@app.command()
def init_test_datasite(
    email: str = typer.Option(..., help="Email address for the client"),
    client_url: str = typer.Option(
        "http://testserver:5000",
        help="URL of the SyftBox client server. Not used when testing locally.",
    ),
    data_dir: Path = typer.Option(..., help="Directory for client data"),
    config_path: Path = typer.Option(..., help="Path to save the config file"),
    overwrite: bool = typer.Option(False, help="Overwrite existing config if present"),
) -> None:
    """
    Initialize a new SyftBox client config for a datasite.
    """
    config_path = config_path.resolve()
    data_dir = data_dir.resolve()

    if config_path.exists() and not overwrite:
        logger.warning(
            f"Config file {config_path} already exists. Use --overwrite to replace."
        )
        typer.echo(f"Config file {config_path} already exists. No action taken.")
        raise typer.Exit(code=0)

    config = SyftClientConfig(
        email=email,
        client_url=client_url,
        path=config_path,
        data_dir=data_dir,
    )
    config.save()
    typer.echo(f"Created new SyftBox client config at {config_path}")


@app.callback()
def callback():
    # Empty command to enable subcommands
    pass


def main():
    app()


if __name__ == "__main__":
    main()
