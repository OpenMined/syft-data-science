import os
from pathlib import Path
from loguru import logger
from syft_core import SyftBoxURL
from syft_event import SyftEvents
from syft_event.types import Request
import yaml
from syft_rds.models.models import (
    Dataset,
    GetAllRequest,
    GetOneRequest,
    ItemList,
    Job,
    JobCreate,
    JobUpdate,
    JobStatus,
)
from syft_rds.server.router import RPCRouter
from syft_rds.server.user_file_service import UserFileService
from syft_rds.store import YAMLStore
from syft_rds.utils.name_generator import generate_name
from syft_rds.utils.zip_utils import zip_to_bytes
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

job_router = RPCRouter()


@job_router.on_request("/create")
def create_job(create_request: JobCreate, app: SyftEvents, request: Request) -> Job:
    user = request.sender  # TODO auth
    job_store: YAMLStore[Job] = app.state["job_store"]
    user_file_service: UserFileService = app.state["user_file_service"]

    create_request.name = create_request.name or generate_name()
    new_item = create_request.to_item(extra={"created_by": user})
    # Create the output directory, user_file_service makes it readable for the user who created the job
    job_output_dir = user_file_service.dir_for_item(
        user=user,
        item=new_item,
    )
    new_item.output_url = SyftBoxURL.from_path(job_output_dir, app.client.workspace)

    job_res = job_store.create(new_item)

    _handle_auto_approval(create_request, job_res, app, request)

    return job_res


def _handle_auto_approval(
    create_request: JobCreate, job_res: Job, app: SyftEvents, request: Request
) -> None:
    dataset_store: YAMLStore[Dataset] = app.state["dataset_store"]
    dataset: Dataset = dataset_store.get_one(name=create_request.dataset_name)

    if request.sender in dataset.auto_approval:
        try:
            job_update = JobUpdate(uid=job_res.uid, status=JobStatus.approved)
            update_job(job_update, app)
        except Exception as e:
            logger.error(f"Failed to auto-approve job {job_res.uid}: {e}")


@job_router.on_request("/get_one")
def get_job(request: GetOneRequest, app: SyftEvents) -> Job:
    job_store: YAMLStore[Job] = app.state["job_store"]
    filters = request.filters
    if request.uid is not None:
        filters["uid"] = request.uid
    item = job_store.get_one(**filters)
    if item is None:
        raise ValueError(f"No job found with filters {filters}")
    return item


@job_router.on_request("/get_all")
def get_all_jobs(req: GetAllRequest, app: SyftEvents) -> ItemList[Job]:
    job_store: YAMLStore[Job] = app.state["job_store"]
    items = job_store.get_all(
        limit=req.limit,
        offset=req.offset,
        order_by=req.order_by,
        sort_order=req.sort_order,
        filters=req.filters,
    )
    return ItemList[Job](items=items)


@job_router.on_request("/update")
def update_job(update_request: JobUpdate, app: SyftEvents) -> Job:
    job_store: YAMLStore[Job] = app.state["job_store"]
    existing_item = job_store.get_by_uid(update_request.uid)
    if existing_item is None:
        raise ValueError(f"Job with uid {update_request.uid} not found")

    if existing_item.enclave:
        _handle_enclave_update(existing_item, app)

    updated_item = existing_item.apply_update(update_request)
    return job_store.update(updated_item.uid, updated_item)


def encrypt_data(data: bytes, public_key_path: Path, output_file_path: Path) -> bytes:
    """Encrypt data using a public key and save it to a file."""

    with open(public_key_path, "rb") as key_file:
        public_key = serialization.load_pem_public_key(
            key_file.read(),
        )

    # Step 1: Generate a random AES key
    aes_key = AESGCM.generate_key(bit_length=256)

    # Step 2: Encrypt the zip data with AES-GCM
    aesgcm = AESGCM(aes_key)
    nonce = os.urandom(12)  # GCM standard nonce size
    ciphertext = aesgcm.encrypt(nonce, data, associated_data=None)

    # Step 3: Encrypt the AES key with RSA public key
    encrypted_key = public_key.encrypt(
        aes_key,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )

    # Step 4: Save (or send) nonce + encrypted AES key + ciphertext
    with open(output_file_path, "wb") as f:
        f.write(len(encrypted_key).to_bytes(2, "big"))  # length of key prefix
        f.write(encrypted_key)
        f.write(nonce)
        f.write(ciphertext)


def _handle_enclave_update(existing_item: Job, app: SyftEvents) -> None:
    client = app.client

    enclave_data_dir = client.app_data("enclave") / "data" / existing_item.enclave
    enclave_data_dir.mkdir(parents=True, exist_ok=True)
    add_permission_rule(
        path=enclave_data_dir,
        pattern="**",
        read=[existing_item.enclave],
        write=[],
    )

    dataset_store: YAMLStore[Dataset] = app.state["dataset_store"]
    dataset: Dataset = dataset_store.get_one(name=existing_item.dataset_name)
    dataset_private_path = dataset.private.to_local_path(app.client.datasites)
    if not dataset_private_path.exists():
        raise ValueError(f"Dataset Private Path {dataset_private_path} does not exist")
    zip_bytes = zip_to_bytes(
        files_or_dirs=dataset_private_path,
        base_dir=dataset_private_path,
    )

    enclave_public_key = (
        client.app_data("enclave", datasite=existing_item.enclave)
        / "keys"
        / "public_key.pem"
    )

    if not enclave_public_key.exists():
        raise ValueError(f"Public key for enclave {existing_item.enclave} not found")

    output_file_path = enclave_data_dir / f"{dataset.uid}.enc"

    encrypt_data(
        data=zip_bytes,
        public_key_path=enclave_public_key,
        output_file_path=output_file_path,
    )


# TODO: Move this to syft core
def add_permission_rule(path: str, pattern: str, read: list[str], write: list[str]):
    """
    Adds or updates a permission rule in syft.pub.yaml in the given path (must be a folder).
    If a rule with the same pattern exists, update only missing read/write entries (no duplicates).
    Raises ValueError if path is not a directory.
    """
    folder = Path(path)
    if not folder.is_dir():
        raise ValueError(f"Provided path '{path}' is not a directory.")
    yaml_file = folder / "syft.pub.yaml"
    rule = {"pattern": pattern, "access": {"read": read, "write": write}}
    if yaml_file.exists():
        with open(yaml_file, "r") as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {}
    if "rules" not in data or not isinstance(data["rules"], list):
        data["rules"] = []
    # Check for existing pattern
    for existing_rule in data["rules"]:
        if existing_rule.get("pattern") == pattern:
            # Update read and write lists, avoiding duplicates
            existing_access = existing_rule.setdefault("access", {})
            for key, new_values in [("read", read), ("write", write)]:
                existing = set(existing_access.get(key, []))
                updated = list(existing.union(new_values))
                existing_access[key] = updated
            break
    else:
        # No existing pattern, append new rule
        data["rules"].append(rule)
    with open(yaml_file, "w") as f:
        yaml.safe_dump(data, f, sort_keys=False)
