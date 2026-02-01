import contextlib
import sys
from collections.abc import AsyncGenerator, Generator
from typing import TYPE_CHECKING

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.shared.wait import async_wait_for_true
from tests.conftest import should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

if TYPE_CHECKING:
    from key_value.aio.stores.aerospike import AerospikeStore

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="Aerospike is not supported on Windows")

# Aerospike test configuration
AEROSPIKE_NAMESPACE = "test"
AEROSPIKE_SET = "kv-store-adapter-tests"

WAIT_FOR_AEROSPIKE_TIMEOUT = 30

AEROSPIKE_CONTAINER_PORT = 3000


async def ping_aerospike(host: str, port: int) -> bool:
    from key_value.aio.stores.aerospike.store import (
        _close_aerospike_client,
        _connect_aerospike_client,
        _create_aerospike_client,
    )

    try:
        config = {"hosts": [(host, port)]}
        client = _create_aerospike_client(config)
        _connect_aerospike_client(client)
        _close_aerospike_client(client)
    except Exception:
        return False
    else:
        return True


class AerospikeFailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestAerospikeStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="module")
    def aerospike_container(self) -> Generator[DockerContainer, None, None]:
        container = DockerContainer(image="aerospike/aerospike-server:latest")
        container.with_exposed_ports(AEROSPIKE_CONTAINER_PORT)
        # DEFAULT_TTL must be non-zero to allow TTL writes (0 rejects TTL writes with FORBIDDEN error)
        # NSUP_PERIOD enables TTL expiration (namespace supervisor runs every N seconds)
        container.with_env("DEFAULT_TTL", "86400")
        container.with_env("NSUP_PERIOD", "1")
        container.waiting_for(LogMessageWaitStrategy("service ready: soon there will be cake!"))
        with container:
            yield container

    @pytest.fixture(scope="module")
    def aerospike_host(self, aerospike_container: DockerContainer) -> str:
        return aerospike_container.get_container_host_ip()

    @pytest.fixture(scope="module")
    def aerospike_port(self, aerospike_container: DockerContainer) -> int:
        return int(aerospike_container.get_exposed_port(AEROSPIKE_CONTAINER_PORT))

    @pytest.fixture(autouse=True, scope="module")
    async def setup_aerospike(self, aerospike_container: DockerContainer, aerospike_host: str, aerospike_port: int) -> None:
        async def _ping() -> bool:
            return await ping_aerospike(aerospike_host, aerospike_port)

        ready = await async_wait_for_true(
            bool_fn=_ping,
            tries=WAIT_FOR_AEROSPIKE_TIMEOUT,
            wait_time=1,
        )
        if not ready:
            msg = "Aerospike failed to start"
            raise AerospikeFailedToStartError(msg)

    @override
    @pytest.fixture
    async def store(self, setup_aerospike: None, aerospike_host: str, aerospike_port: int) -> AsyncGenerator["AerospikeStore", None]:
        from key_value.aio.stores.aerospike import AerospikeStore
        from key_value.aio.stores.aerospike.store import (
            _close_aerospike_client,
            _connect_aerospike_client,
            _create_aerospike_client,
            _truncate_aerospike_set,
        )

        config = {"hosts": [(aerospike_host, aerospike_port)]}
        client = _create_aerospike_client(config)
        _connect_aerospike_client(client)

        # Use a unique set name per test worker to avoid conflicts with parallel execution
        import uuid

        unique_set = f"{AEROSPIKE_SET}-{uuid.uuid4().hex[:8]}"
        store = AerospikeStore(client=client, namespace=AEROSPIKE_NAMESPACE, set_name=unique_set)

        yield store

        # Clean up the set after tests
        with contextlib.suppress(Exception):
            _truncate_aerospike_set(client, AEROSPIKE_NAMESPACE, unique_set)
        _close_aerospike_client(client)

    @pytest.fixture
    async def aerospike_store(self, store: "AerospikeStore") -> "AerospikeStore":
        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
