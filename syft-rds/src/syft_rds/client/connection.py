from abc import ABC, abstractmethod
from datetime import datetime, timezone

from pydantic import BaseModel
from syft_core import Client as SyftBoxClient
from syft_core import SyftBoxURL
from syft_event import SyftEvents
from syft_event.deps import func_args_from_request
from syft_rpc import SyftRequest
from syft_rpc.protocol import SyftMethod
from syft_rpc.rpc import parse_duration, serialize


class RPCConnection(ABC):
    def __init__(self, sender_client: SyftBoxClient):
        self.sender_client = sender_client

    @abstractmethod
    def send(self, url: str, body: BaseModel, expiry: str, cache: bool):
        raise NotImplementedError("This is an abstract class")


class CachingServerRPCConnection(RPCConnection):
    def send(
        self,
        url: str,
        body: BaseModel,
        expiry: str,
        cache: bool,
    ):
        raise NotImplementedError("TODO")


class DevRPCConnection(RPCConnection):
    app: SyftEvents
    sender_client: SyftBoxClient

    def __init__(self, sender_client: SyftBoxClient, app: SyftEvents):
        self.app = app
        super().__init__(sender_client)

    @property
    def receiver_client(self) -> SyftBoxClient:
        return self.app.client

    def send(self, url: str, body: BaseModel, expiry: str, cache: bool):
        headers = None

        syft_request = SyftRequest(
            sender=self.sender_client.email,
            method=SyftMethod.GET,
            url=url if isinstance(url, SyftBoxURL) else SyftBoxURL(url),
            headers=headers or {},
            body=serialize(body),
            expires=datetime.now(timezone.utc) + parse_duration(expiry),
        )
        receiver_local_path = SyftBoxURL(url).to_local_path(
            self.receiver_client.workspace.datasites
        )

        handler = self.app._SyftEvents__rpc.get(receiver_local_path)
        if handler is None:
            raise ValueError(
                f"No handler found for URL: {url}, got {self.app._SyftEvents__rpc.keys()}"
            )
        kwargs = func_args_from_request(
            handler,
            syft_request,
            self.app,
        )

        response = handler(**kwargs)
        return response


def get_connection(
    sender_client: SyftBoxClient, app: SyftEvents, mock=True
) -> RPCConnection:
    if mock:
        # TODO
        conn = DevRPCConnection(sender_client=sender_client, app=app)
        return conn
    else:
        return CachingServerRPCConnection(sender_client=sender_client)
