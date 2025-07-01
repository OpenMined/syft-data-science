from functools import wraps
from typing import Any, Callable

from syft_rds.client.rds_clients.base import RDSClientModule


def ensure_is_admin(func: Callable) -> Callable:
    """
    Decorator to ensure the user is an admin before executing a function.
    Admin status is determined by comparing the SyftBox client email with the configured host.
    """

    @wraps(func)
    def wrapper(self: RDSClientModule, *args: Any, **kwargs: Any) -> Callable:
        if not self.is_admin:
            raise PermissionError(
                f"You must be the datasite admin to perform this operation. "
                f"Your SyftBox email: '{self._syftbox_client.email}'. "
                f"Host email: '{self.config.host}'"
            )
        return func(self, *args, **kwargs)

    return wrapper
