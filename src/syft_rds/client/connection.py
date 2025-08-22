from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional

from syft_core import Client as SyftBoxClient
from syft_core import SyftBoxURL
from syft_core.permissions import (
    get_computed_permission,
    ComputedPermission,
    PermissionType,
)
from syft_core.exceptions import SyftBoxException
from syft_core.types import RelativePath
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

    def _serialize(self, body: BodyType) -> str:
        # NOTE to enable partial BaseModel updates, always exclude unset fields when serializing
        # exclude_unset will only affect the serialization of Pydantic models
        return rpc.serialize(body, exclude_unset=True)


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

        body = self._serialize(body)
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


def check_permission(
    client: SyftBoxClient,
    path: str,
    permission: PermissionType,
) -> bool:
    """
    Check if the client has the permission to access the url.
    If the client does not have the permission, we will raise an error *in future*,
    but for now we will just log a warning.
    """
    try:
        relative_path = RelativePath(path)
    except ValueError:
        path = path.relative_to(client.workspace.datasites)
        relative_path = RelativePath(path)
    sender_permission: ComputedPermission = get_computed_permission(
        client=client, path=relative_path
    )
    has_permission = sender_permission.has_permission(permission)
    if not has_permission:
        raise SyftBoxException(
            f"User {client.email} does not have {permission} permission for {path}"
        )

    return has_permission


class MockRPCConnection(BlockingRPCConnection):
    app: SyftEvents
    sender_client: SyftBoxClient

    def __init__(self, sender_client: SyftBoxClient, app: SyftEvents):
        self.app = app
        self.app.init()
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

        # NOTE to match the FileSyncRPCConnection.send implementation, we self._serialize the body here with our custom serde options
        # in rpc.send the body will be serialized again which will be a no-op when building the request. This is a no-op on already serialized data.
        body = self._serialize(body)
        syft_request = self._build_request(url, body, headers, expiry)
        syft_url = SyftBoxURL(url)

        req_path = (
            syft_url.to_local_path(self.sender_client.workspace.datasites)
            / f"{syft_request.id}.request"
        )

        req_path = req_path.relative_to(self.sender_client.workspace.datasites)

        # check_permission(self.sender_client, req_path, PermissionType.WRITE)
        # check_permission(self.receiver_client, req_path, PermissionType.READ)

        receiver_local_path = syft_url.to_local_path(
            self.receiver_client.workspace.datasites
        )
        handler = self.app.get_handler(receiver_local_path)
        if handler is None:
            raise ValueError(f"No handler found for: {receiver_local_path}")
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
