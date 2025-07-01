from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClient


class GlobalClientRegistry:
    registry = {}

    @classmethod
    def get_client(cls, uid: UUID) -> "RDSClient":
        return cls.registry[uid]

    @classmethod
    def register_client(cls, client: "RDSClient") -> None:
        cls.registry[client.uid] = client
