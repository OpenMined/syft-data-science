from datetime import datetime, timezone

from pydantic import BaseModel
from syft_core import Client, SyftBoxURL
from syft_event import SyftEvents
from syft_event.deps import func_args_from_request
from syft_rpc import SyftRequest
from syft_rpc.protocol import SyftMethod
from syft_rpc.rpc import parse_duration, serialize


class RPCConnection(BaseModel):
    def send(
        self, url: str, body: BaseModel, expiry: str, cache: bool, client: Client = None
    ):
        raise NotImplementedError("This is an abstract class")


class CachingServerRPCConnection(RPCConnection):
    def send(
        self, url: str, body: BaseModel, expiry: str, cache: bool, client: Client = None
    ):
        pass
        # syft_rpc.send()


class DevRPCConnection(RPCConnection):
    _app: SyftEvents

    def send(
        self, url: str, body: BaseModel, expiry: str, cache: bool, client: Client = None
    ):
        # Get the endpoint handler from the app's registered RPCs
        client = Client.load() if client is None else client

        headers = None

        syft_request = SyftRequest(
            sender=client.email,
            method=SyftMethod.GET,
            url=url if isinstance(url, SyftBoxURL) else SyftBoxURL(url),
            headers=headers or {},
            body=serialize(body),
            expires=datetime.now(timezone.utc) + parse_duration(expiry),
        )
        local_path = SyftBoxURL(url).to_local_path(client.workspace.datasites)

        handler = self._app._SyftEvents__rpc.get(local_path)
        if handler is None:
            raise ValueError(
                f"No handler found for URL: {url}, got {self._app._SyftEvents__rpc.keys()}"
            )
        kwargs = func_args_from_request(handler, syft_request)

        response = handler(**kwargs)
        return response


def get_connection(app: SyftEvents, mock=True) -> RPCConnection:
    if mock:
        # TODO
        res = DevRPCConnection(_app=app)
        res._app = app
        return res
    else:
        return CachingServerRPCConnection()
