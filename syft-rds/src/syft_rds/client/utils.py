import os
from pathlib import Path
from typing import TypeAlias, Union


PathLike: TypeAlias = Union[str, os.PathLike, Path]


def to_path(path: PathLike) -> Path:
    return Path(path).expanduser().resolve()
