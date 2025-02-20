from typing import Optional
from syft_core import Client


PERMS = """
- path: '**'
  permissions:
  - admin
  - read
  - write
  user: {email}
- path: '**'
  permissions:
  - read
  user: '*'
"""


class RDSStore:
    def __init__(
        self,
        app_name: str,
        client: Optional[Client] = None,
    ):
        self.app_name = app_name
        if client is None:
            self.client = Client.load()
        self.app_dir = self.client.api_data(self.app_name)
        self.app_store_dir = self.app_dir / "store"

        # Create app store directory
        self.app_store_dir.mkdir(exist_ok=True, parents=True)

        # Create READ Permissions for all users in the store Director
        perms = self.app_store_dir / "syftperm.yaml"
        perms.write_text(PERMS.format(email=self.client.email))

    @property
    def store_dir(self):
        return self.app_store_dir
