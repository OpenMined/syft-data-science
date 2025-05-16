import asyncio
import os
import shutil
import sys
from asyncio.subprocess import Process
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from syft_core import SyftClientConfig, SyftWorkspace

import httpx
import pytest_asyncio
from loguru import logger

logger.remove()
logger.add(
    sys.stderr,
    format="<level>{level: <8}</level> | <level>{message}</level>",
    colorize=True,
)
logger.level("SUCCESS", color="<b><green>")
logger.level("INFO", color="<b><cyan>")
logger.level("DEBUG", color="<blue>")


def _get_random_port() -> int:
    """Get a random available port by binding to port 0.

    Returns:
        int: A random available port number
    """
    import socket

    # Try up to 10 times to get an available port
    for attempt in range(10):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # Bind to port 0 to get a random available port
                s.bind(("localhost", 0))
                port = s.getsockname()[1]
                # Set SO_REUSEADDR to allow immediate reuse of the port
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                return port
        except OSError as e:
            if attempt == 9:  # Last attempt
                logger.error(f"Failed to find available port after 10 attempts: {e}")
                raise RuntimeError("Could not find an available port after 10 attempts")
            continue

    raise RuntimeError("Could not find an available port after 10 attempts")


class E2ETestError(Exception):
    pass


class E2ETimeoutError(E2ETestError):
    pass


@dataclass
class Server:
    port: int = field(default_factory=lambda: _get_random_port())
    env: Dict[str, str] = field(default_factory=dict)


@dataclass
class Client:
    name: str
    server_port: int
    port: int = field(default_factory=lambda: _get_random_port())
    data_dir: Path = field(default_factory=Path.cwd)  # Set by E2EContext
    env: Dict[str, str] = field(default_factory=dict)
    apps: List[str] = field(default_factory=list)

    @property
    def config(self):
        return SyftClientConfig(
            email=self.email,
            client_url=f"http://localhost:{self.port}",
            path=self.config_path,
            data_dir=self.data_dir,
        )

    @property
    def workspace(self):
        return SyftWorkspace(self.config.data_dir)

    @property
    def email(self):
        return f"{self.name}@openmined.org"

    @property
    def datasite_dir(self):
        """data_dir/datasites"""
        return self.data_dir / "datasites"

    @property
    def api_dir(self):
        """DEPRECATED: use app_dir"""
        return self.app_dir

    @property
    def app_dir(self):
        """data_dir/apps"""
        return self.data_dir / "apps"

    @property
    def private_dir(self):
        """data_dir/private"""
        return self.data_dir / "private"

    @property
    def my_datasite(self):
        """data_dir/datasites/{email}"""
        return self.datasite_dir / self.email

    @property
    def config_path(self):
        """data_dir/datasites/{email}"""
        return self.data_dir / "config.json"

    @property
    def public_dir(self):
        """data_dir/datasites/{email}/public"""
        return self.my_datasite / "public"

    @property
    def api_request_name(self) -> str:
        """DEPRECATED: use app_request_name"""
        return self.app_request_name

    @property
    def app_request_name(self) -> str:
        app_path = Path.cwd()
        app_name = app_path.name
        return app_name

    def api_data(
        self,
        api_request_name: Optional[str] = None,
        datasite: Optional[str] = None,
    ) -> Path:
        """DEPRECATED: use app_data"""
        return self.app_data(api_request_name, datasite)

    def app_data(
        self,
        app_request_name: Optional[str] = None,
        datasite: Optional[str] = None,
    ) -> Path:
        app_request_name = app_request_name or self.app_request_name
        datasite = datasite or self.email
        return self.datasite_dir / datasite / "app_data" / app_request_name


class E2EContext:
    def __init__(self, e2e_name: str, server: Server, clients: List[Client]):
        self.e2e_name = e2e_name
        self.test_dir = Path.cwd() / ".e2e" / e2e_name
        logger.info(f"TEST DIR: {self.test_dir}")
        self.server = server
        self.clients = clients
        self.__procs: List[Process] = []

    def reset_test_dir(self):
        shutil.rmtree(self.test_dir.parent, ignore_errors=True)

    async def start_all(self) -> bool:
        # Start server
        await self.start_server(self.server)
        await self.wait_for_server(self.server)

        # Start clients
        await asyncio.gather(*[self.start_client(c) for c in self.clients])
        await self.wait_for_clients(self.clients)

        return True

    async def cleanup(self):
        for process in self.__procs:
            if process.returncode is None:
                process.kill()
                await process.wait()
        for path in Path(".e2e").rglob("*.pid"):
            try:
                path.unlink()
            except Exception:
                pass

    async def start_server(self, server: Server) -> Process:
        server_dir = self.test_dir / "server"
        server_dir.mkdir(parents=True, exist_ok=True)

        logs_dir = self.test_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()
        env["SYFTBOX_ENV"] = "DEV"
        env["SYFTBOX_DATA_FOLDER"] = str(server_dir)
        env["SYFTBOX_OTEL_ENABLED"] = "0"
        env.update(server.env)

        process = await asyncio.create_subprocess_exec(
            "gunicorn",
            "syftbox.server.server:app",
            "-k=uvicorn.workers.UvicornWorker",
            f"--bind=127.0.0.1:{server.port}",
            stdout=open(logs_dir / "server.log", "w"),
            stderr=asyncio.subprocess.STDOUT,
            env=env,
        )

        self.__procs.append(process)

        return process

    async def start_client(self, client: Client) -> Process:
        client_dir = self.test_dir / "clients" / client.name
        client_dir.mkdir(parents=True, exist_ok=True)
        client.data_dir = client_dir

        logs_dir = self.test_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()
        env["SYFTBOX_DISABLE_ICONS"] = "1"
        env["CLIENT_CONFIG_PATH"] = f"{client_dir}/config.json"
        env.update(client.env)
        if len(client.apps) > 0:
            env["SYFTBOX_DEFAULT_APPS"] = ",".join(client.apps)
        logger.info(f"{client_dir=}")

        process = await asyncio.create_subprocess_exec(
            "syftbox",
            "client",
            f"--email={client.name}@openmined.org",
            f"--config_path={client_dir}/config.json",
            f"--sync_folder={client_dir}",
            f"--server=http://localhost:{client.server_port}",
            f"--port={client.port}",
            "--no-open-dir",
            "--verbose",
            stdout=open(logs_dir / f"client.{client.name}.log", "w"),
            stderr=asyncio.subprocess.STDOUT,
            env=env,
        )
        self.__procs.append(process)
        return process

    async def wait_for_server(self, server: Server, timeout: int = 30):
        logger.debug(
            f"Waiting for server to be ready on port {server.port} (timeout={timeout}s)"
        )
        await self.wait_for_url(f"http://localhost:{server.port}/info", timeout=timeout)
        logger.success(f"Server '{server.port}' is ready")

    async def wait_for_clients(self, clients: List[Client], timeout: int = 30):
        # wait for all futures with a timeout
        await asyncio.gather(
            *[self.wait_for_client(c, timeout=timeout) for c in clients]
        )

    async def wait_for_client(self, client: Client, timeout: int = 30):
        logger.debug(
            f"Waiting for client '{client.name}' to be ready on port {client.port} (timeout={timeout}s)"
        )
        await self.wait_for_url(f"http://localhost:{client.port}/info", timeout=timeout)
        logger.success(
            f"Client '{client.name}' is ready and connecting to server at port {client.server_port}"
        )

    async def wait_for_app(self, app_name: str, client: Client, timeout: int = 30):
        logger.debug(f"Waiting for APP '{app_name}' to be ready (timeout={timeout}s)")
        run_path = client.app_dir / app_name / "run.sh"
        await self.wait_for_path(run_path, timeout=timeout)
        logger.success(f"APP '{app_name}' is ready on client '{client.name}'")

    async def wait_for_path(
        self, path: Path, timeout: int = 30, interval: float = 0.5
    ) -> None:
        logger.debug(f"Waiting for path '{path}' (timeout={timeout}s)")
        start = asyncio.get_event_loop().time()

        while not path.exists():
            if asyncio.get_event_loop().time() - start > timeout:
                raise E2ETimeoutError(f"Timeout after {timeout}s waiting for: {path}")
            await asyncio.sleep(interval)

        elapsed = asyncio.get_event_loop().time() - start
        logger.debug(f"Got {path} (after {elapsed:.1f}s)")

    async def wait_for_url(
        self, url: str, timeout: int = 30, interval: float = 1.0
    ) -> None:
        async with httpx.AsyncClient() as client:
            start = asyncio.get_event_loop().time()
            while True:
                try:
                    await client.get(url)
                    break
                except httpx.RequestError:
                    if asyncio.get_event_loop().time() - start > timeout:
                        raise E2ETimeoutError(
                            f"Timeout after {timeout}s waiting for: {url}"
                        )
                    await asyncio.sleep(interval)

            elapsed = asyncio.get_event_loop().time() - start
            logger.debug(f"Got response from {url} (after {elapsed:.1f}s)")


@pytest_asyncio.fixture(loop_scope="function")
async def e2e_context(request):
    try:
        ctx = E2EContext(**request.param)
        yield ctx
    except:
        raise
    finally:
        await ctx.cleanup()
