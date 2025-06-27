from pathlib import Path

import pydantic
from syft_core import Client as SyftBoxClient
from syft_core.types import PathLike, to_path
from syft_core.url import SyftBoxURL

from syft_datasets import get_syftbox_client


class Dataset(pydantic.BaseModel):
    name: str
    summary: str | None = None
    readme_url: SyftBoxURL | None = None
    tags: list[str] = []

    public_url: SyftBoxURL
    private_url: SyftBoxURL

    private_location: str | None = None  # high-1234

    @property
    def public_dir(self) -> Path:
        syftbox_client = get_syftbox_client()
        return self.public_url.to_local_path(datasites_path=syftbox_client.datasites)

    @property
    def private_dir(self) -> Path:
        syftbox_client = get_syftbox_client()
        return self.private_dir.to_local_path(datasites_path=syftbox_client.datasites)

    @property
    def readme_path(self) -> Path | None:
        if self.readme_url is None:
            return None
        syftbox_client = get_syftbox_client()
        return self.readme_url.to_local_path(datasites_path=syftbox_client.datasites)


def create(
    name: str,
    public_dir: PathLike,
    private_dir: PathLike,
    readme_path: PathLike | None = None,
    summary: str | None = None,
    tags: list[str] | None = None,
    move_private_to_syftbox: bool = True,
    syftbox_client: SyftBoxClient | None = None,
) -> Dataset:
    if syftbox_client is None:
        syftbox_client = get_syftbox_client()

    public_dir = to_path(public_dir)
    private_dir = to_path(private_dir)
