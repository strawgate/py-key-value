from __future__ import annotations

import os
import socket
import sys
from typing import TYPE_CHECKING, cast
from unittest.mock import MagicMock

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from typing_extensions import override

from key_value.aio._utils.wait import async_wait_for_true
from tests.conftest import should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator

    from key_value.aio.stores.base import BaseStore
    from key_value.aio.stores.valkey import ValkeyStore

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="Valkey is not supported on Windows")

VALKEY_CLUSTER_IMAGE = "valkey/valkey:8.0.0"
VALKEY_CLUSTER_NODE_COUNT = 6
VALKEY_CLUSTER_WAIT_TIMEOUT = 60
VALKEY_CLUSTER_REQUEST_TIMEOUT_MS = 5000
VALKEY_CLUSTER_PORT_START = 17000
VALKEY_CLUSTER_PORT_BLOCK_SIZE = 100
VALKEY_CLUSTER_PORT_BLOCK_COUNT = 70


def _pytest_worker_index() -> int:
    """Return the xdist worker index so parallel workers choose disjoint ports."""
    worker = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
    if not worker.startswith("gw"):
        return 0

    try:
        return int(worker[2:])
    except ValueError:
        return 0


def _ports_are_available(ports: list[int]) -> bool:
    """Return True when every requested host port can be bound."""
    sockets: list[socket.socket] = []
    try:
        for port in ports:
            sock = socket.socket()
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("127.0.0.1", port))
            sockets.append(sock)
    except OSError:
        return False
    finally:
        for sock in sockets:
            sock.close()

    return True


def _find_free_port_block(*, node_count: int = VALKEY_CLUSTER_NODE_COUNT) -> list[int]:
    """Find a host/container port block for a single-container Valkey cluster."""
    worker_count = max(int(os.environ.get("PYTEST_XDIST_WORKER_COUNT", "1")), 1)
    worker_index = _pytest_worker_index() % worker_count

    for block_index in range(worker_index, VALKEY_CLUSTER_PORT_BLOCK_COUNT, worker_count):
        base_port = VALKEY_CLUSTER_PORT_START + (block_index * VALKEY_CLUSTER_PORT_BLOCK_SIZE)
        ports = list(range(base_port, base_port + node_count))
        bus_ports = [port + 10000 for port in ports]
        if _ports_are_available([*ports, *bus_ports]):
            return ports

    msg = "Could not find a free port block for Valkey cluster tests"
    raise RuntimeError(msg)


def _make_valkey_cluster_command(ports: list[int]) -> str:
    """Build the command that starts and clusters multiple Valkey nodes."""
    port_args = " ".join(str(port) for port in ports)
    cluster_nodes = " ".join(f"127.0.0.1:{port}" for port in ports)
    commands = [
        f"for p in {port_args}; do "
        "mkdir -p /tmp/valkey-$p; "
        "valkey-server "
        "--port $p "
        "--cluster-enabled yes "
        "--cluster-config-file nodes-$p.conf "
        "--cluster-node-timeout 5000 "
        "--appendonly no "
        "--protected-mode no "
        "--bind 0.0.0.0 "
        "--dir /tmp/valkey-$p "
        "--cluster-announce-ip 127.0.0.1 "
        "--cluster-announce-port $p "
        "--cluster-announce-bus-port $((p+10000)) "
        "> /tmp/valkey-$p.log 2>&1 & "
        "done",
        f"for p in {port_args}; do "
        "tries=0; "
        "until valkey-cli -h 127.0.0.1 -p $p ping >/dev/null 2>&1; do "
        "tries=$((tries+1)); "
        "if [ $tries -ge 50 ]; then echo valkey node $p failed to start; exit 1; fi; "
        "sleep 0.2; "
        "done; "
        "done",
        f"yes yes | valkey-cli --cluster create {cluster_nodes} --cluster-replicas 1",
        "echo CLUSTER_READY",
        "tail -f /tmp/valkey-*.log",
    ]
    return "sh -ec '" + "; ".join(commands) + "'"


def _make_valkey_cluster_config(port: int):
    """Create a GLIDE cluster config seeded from a local cluster node."""
    from glide import GlideClusterClientConfiguration, NodeAddress

    return GlideClusterClientConfiguration(
        addresses=[NodeAddress("127.0.0.1", port)],
        request_timeout=VALKEY_CLUSTER_REQUEST_TIMEOUT_MS,
    )


async def _ping_valkey_cluster(port: int) -> bool:
    """Return True once GLIDE can connect to the Valkey cluster."""
    from glide.glide_client import GlideClusterClient

    client = None
    try:
        client = await GlideClusterClient.create(config=_make_valkey_cluster_config(port))
        await client.ping()
    except Exception:
        return False
    else:
        return True
    finally:
        if client is not None:
            await client.close()


class TestValkeyClusterClientSupport:
    """Tests for GlideClusterClient type compatibility with ValkeyStore."""

    def test_find_free_port_block_uses_xdist_worker_partition(self, monkeypatch: pytest.MonkeyPatch):
        """Verify parallel pytest workers use disjoint Valkey cluster port blocks."""
        checked_ports: list[list[int]] = []

        def ports_are_available(ports: list[int]) -> bool:
            checked_ports.append(ports)
            return True

        monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw2")
        monkeypatch.setenv("PYTEST_XDIST_WORKER_COUNT", "4")
        monkeypatch.setattr(sys.modules[__name__], "_ports_are_available", ports_are_available)

        ports = _find_free_port_block()

        assert ports == list(range(17200, 17206))
        assert checked_ports == [list(range(17200, 17206)) + list(range(27200, 27206))]

    def test_cluster_command_polls_nodes_before_cluster_create(self):
        """Verify the cluster command waits for each node before clustering."""
        command = _make_valkey_cluster_command([17000, 17001])

        assert "sleep 2" not in command
        assert "valkey-cli -h 127.0.0.1 -p $p ping" in command
        assert command.index("valkey-cli -h 127.0.0.1 -p $p ping") < command.index("valkey-cli --cluster create")

    async def test_cluster_client_type_accepted(self):
        """Verify that ValkeyStore's type hints accept GlideClusterClient.

        This test verifies that the type system recognizes GlideClusterClient
        as a valid client type for ValkeyStore. It does not test runtime
        functionality against a live cluster.
        """
        from glide import GlideClusterClient

        from key_value.aio.stores.valkey import ValkeyStore

        # Verify the import works
        assert GlideClusterClient is not None

        # Type checker should accept this (verified at typecheck time)
        # This line demonstrates that ValkeyStore.__init__ accepts GlideClusterClient
        # We don't actually call it because we don't have a live cluster
        _: type[ValkeyStore] = ValkeyStore

        # Verify GlideClusterClient is imported in the store module
        from key_value.aio.stores.valkey.store import GlideClusterClient as ImportedClient

        assert ImportedClient is GlideClusterClient

    async def test_cluster_config_type_accepted(self):
        """Verify that ValkeyStore accepts a cluster configuration."""
        from glide import GlideClusterClientConfiguration, NodeAddress

        from key_value.aio.stores.valkey import ValkeyStore

        config = GlideClusterClientConfiguration(addresses=[NodeAddress("localhost", 6379)])
        store = ValkeyStore(config=config)

        assert store._client_config is config

    async def test_config_and_client_are_mutually_exclusive(self):
        """Verify that callers cannot provide both a client and config."""
        from glide import GlideClusterClient, GlideClusterClientConfiguration, NodeAddress

        from key_value.aio.stores.valkey import ValkeyStore

        config = GlideClusterClientConfiguration(addresses=[NodeAddress("localhost", 6379)])
        client = MagicMock(spec=GlideClusterClient)

        with pytest.raises(ValueError, match="client and config are mutually exclusive"):
            ValkeyStore(client=client, config=config)  # pyright: ignore[reportCallIssue]

    async def test_standalone_config_creates_standalone_client(self, monkeypatch: pytest.MonkeyPatch):
        """Verify standalone configs still use GlideClient."""
        from glide import GlideClient, GlideClientConfiguration, GlideClusterClient, GlideClusterClientConfiguration, NodeAddress

        from key_value.aio.stores.valkey.store import _create_valkey_client

        calls: list[str] = []

        async def standalone_create(cls: type[GlideClient], config: GlideClientConfiguration) -> GlideClient:
            calls.append("standalone")
            return cast("GlideClient", MagicMock(spec=GlideClient))

        async def cluster_create(cls: type[GlideClusterClient], config: GlideClusterClientConfiguration) -> GlideClusterClient:
            calls.append("cluster")
            return cast("GlideClusterClient", None)

        monkeypatch.setattr(GlideClient, "create", classmethod(standalone_create))
        monkeypatch.setattr(GlideClusterClient, "create", classmethod(cluster_create))

        config = GlideClientConfiguration(addresses=[NodeAddress("localhost", 6379)])
        client = await _create_valkey_client(config)

        assert client is not None
        assert calls == ["standalone"]

    async def test_cluster_config_creates_cluster_client(self, monkeypatch: pytest.MonkeyPatch):
        """Verify cluster configs use GlideClusterClient instead of GlideClient."""
        from glide import GlideClient, GlideClientConfiguration, GlideClusterClient, GlideClusterClientConfiguration, NodeAddress

        from key_value.aio.stores.valkey.store import _create_valkey_client

        calls: list[str] = []

        async def standalone_create(cls: type[GlideClient], config: GlideClientConfiguration) -> GlideClient:
            calls.append("standalone")
            return cast("GlideClient", None)

        async def cluster_create(cls: type[GlideClusterClient], config: GlideClusterClientConfiguration) -> GlideClusterClient:
            calls.append("cluster")
            return cast("GlideClusterClient", MagicMock(spec=GlideClusterClient))

        monkeypatch.setattr(GlideClient, "create", classmethod(standalone_create))
        monkeypatch.setattr(GlideClusterClient, "create", classmethod(cluster_create))

        config = GlideClusterClientConfiguration(addresses=[NodeAddress("localhost", 6379)])
        client = await _create_valkey_client(config)

        assert client is not None
        assert calls == ["cluster"]


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not running")
class TestValkeyClusterStore(ContextManagerStoreTestMixin, BaseStoreTests):
    """Run the base store contract against a local GLIDE cluster client."""

    @pytest.fixture(scope="module")
    def valkey_cluster_ports(self) -> list[int]:
        return _find_free_port_block()

    @pytest.fixture(autouse=True, scope="module")
    def valkey_cluster_container(self, valkey_cluster_ports: list[int]) -> Iterator[DockerContainer]:
        container = (
            DockerContainer(image=VALKEY_CLUSTER_IMAGE)
            .with_command(_make_valkey_cluster_command(valkey_cluster_ports))
            .waiting_for(LogMessageWaitStrategy("CLUSTER_READY").with_startup_timeout(VALKEY_CLUSTER_WAIT_TIMEOUT))
        )

        for port in valkey_cluster_ports:
            container.with_bind_ports(port, port)
            container.with_bind_ports(port + 10000, port + 10000)

        with container:
            yield container

    @pytest.fixture(autouse=True, scope="module")
    async def setup_valkey_cluster(self, valkey_cluster_container: DockerContainer, valkey_cluster_ports: list[int]) -> None:
        ready = await async_wait_for_true(
            bool_fn=lambda: _ping_valkey_cluster(valkey_cluster_ports[0]),
            tries=VALKEY_CLUSTER_WAIT_TIMEOUT,
            wait_time=1,
        )
        if not ready:
            msg = "Valkey cluster failed to start"
            raise RuntimeError(msg)

    @pytest.fixture
    async def store(self, setup_valkey_cluster: None, valkey_cluster_ports: list[int]) -> AsyncGenerator[ValkeyStore, None]:
        from glide.glide_client import GlideClusterClient

        from key_value.aio.stores.valkey import ValkeyStore

        cleanup_client = await GlideClusterClient.create(config=_make_valkey_cluster_config(valkey_cluster_ports[0]))
        try:
            await cleanup_client.flushall()
        finally:
            await cleanup_client.close()

        async with ValkeyStore(config=_make_valkey_cluster_config(valkey_cluster_ports[0])) as store:
            yield store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
