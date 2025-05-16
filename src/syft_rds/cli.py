from pathlib import Path
from typing import Optional

import typer
from syft_core import Client as SyftBoxClient

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


@app.callback()
def callback():
    # Empty command to enable subcommands
    pass


def main():
    app()


if __name__ == "__main__":
    main()
