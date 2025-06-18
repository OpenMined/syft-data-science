import functools
import os
import warnings
from pathlib import Path
from typing import TypeAlias, Union

PathLike: TypeAlias = Union[str, os.PathLike, Path]


def to_path(path: PathLike) -> Path:
    return Path(path).expanduser().resolve()


def deprecation_warning(reason: str = ""):
    """Decorator to mark functions as deprecated with a warning."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            warning_str = f"Function '{func.__name__}' is deprecated and will be removed in a future version."
            if reason:
                warning_str += f" {reason}"
            warnings.warn(
                warning_str,
                category=DeprecationWarning,
                stacklevel=2,
            )
            return func(*args, **kwargs)

        return wrapper

    return decorator
