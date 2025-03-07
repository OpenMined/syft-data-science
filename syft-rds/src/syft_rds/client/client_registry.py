from uuid import UUID
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClient


class GlobalClientRegistry:
    registry = {}

    @classmethod
    def get_client(cls, id: UUID) -> "RDSClient":
        return cls.registry[id]

    @classmethod
    def register_client(cls, id: UUID, client: "RDSClient") -> None:
        cls.registry[id] = client
