from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional

from syft_core import Client as SyftBoxClient
from syft_core import SyftBoxURL
from syft_event import SyftEvents
from syft_event.deps import func_args_from_request
from syft_rpc import SyftRequest, SyftResponse, rpc
from syft_rpc.protocol import SyftMethod, SyftStatus
from syft_rpc.rpc import BodyType


class BlockingRPCConnection(ABC):
    def __init__(
        self,
        sender_client: SyftBoxClient,
        default_expiry: str = "15m",
    ):
        self.sender_client = sender_client
        self.default_expiry = default_expiry

    @abstractmethod
    def send(
        self,
        url: str,
        body: BodyType,
        headers: Optional[dict] = None,
        expiry: Optional[str] = None,
        cache: bool = False,
    ) -> SyftResponse:
        raise NotImplementedError()


class FileSyncRPCConnection(BlockingRPCConnection):
    def send(
        self,
        url: str,
        body: BodyType,
        headers: Optional[dict] = None,
        expiry: Optional[str] = None,
        cache: bool = False,
    ) -> SyftResponse:
        headers = None

        future = rpc.send(
            url=url,
            body=body,
            headers=headers,
            expiry=expiry,
            cache=cache,
            client=self.sender_client,
        )

        timeout_seconds = float(rpc.parse_duration(expiry).seconds)
        response = future.wait(timeout=timeout_seconds)
        return response


class MockRPCConnection(BlockingRPCConnection):
    app: SyftEvents
    sender_client: SyftBoxClient

    def __init__(self, sender_client: SyftBoxClient, app: SyftEvents):
        self.app = app
        super().__init__(sender_client)

    @property
    def receiver_client(self) -> SyftBoxClient:
        return self.app.client

    def _build_request(
        self,
        url: str,
        body: BodyType,
        headers: Optional[dict] = None,
        expiry: Optional[str] = None,
    ) -> SyftRequest:
        headers = None

        expiry_time = datetime.now(timezone.utc) + rpc.parse_duration(expiry)
        return SyftRequest(
            sender=self.sender_client.email,
            method=SyftMethod.GET,
            url=url if isinstance(url, SyftBoxURL) else SyftBoxURL(url),
            headers=headers or {},
            body=rpc.serialize(body),
            expires=expiry_time,
        )

    def _build_response(
        self,
        request: SyftRequest,
        response_body: BodyType,
        status_code: SyftStatus = SyftStatus.SYFT_200_OK,
    ) -> SyftResponse:
        return SyftResponse(
            id=request.id,
            sender=self.receiver_client.email,
            url=request.url,
            headers={},
            body=rpc.serialize(response_body),
            expires=request.expires,
            status_code=status_code,
        )

    def send(
        self,
        url: str,
        body: BodyType,
        headers: Optional[dict] = None,
        expiry: Optional[str] = None,
        cache: bool = False,
    ) -> SyftResponse:
        if cache:
            raise NotImplementedError("Cache not implemented for MockRPCConnection")

        syft_request = self._build_request(url, body, headers, expiry)
        receiver_local_path = SyftBoxURL(url).to_local_path(
            self.receiver_client.workspace.datasites
        )

        handler = self.app._SyftEvents__rpc.get(receiver_local_path)
        if handler is None:
            raise ValueError(
                f"No handler found for: {receiver_local_path}, got {self.app._SyftEvents__rpc.keys()}"
            )
        kwargs = func_args_from_request(handler, syft_request, self.app)

        response_body = handler(**kwargs)
        return self._build_response(syft_request, response_body)


def get_connection(
    sender_client: SyftBoxClient,
    app: SyftEvents,
    mock: bool = False,
) -> BlockingRPCConnection:
    if mock:
        return MockRPCConnection(sender_client=sender_client, app=app)
    else:
        return FileSyncRPCConnection(sender_client=sender_client)
