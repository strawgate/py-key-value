import contextlib
from collections.abc import Generator

import pytest
from key_value.shared.stores.wait import async_wait_for_true
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from typing_extensions import override

from key_value.aio.stores.aerospike import AerospikeStore
from key_value.aio.stores.base import BaseStore
from tests.conftest import should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

# Aerospike test configuration
AEROSPIKE_NAMESPACE = "test"
AEROSPIKE_SET = "kv-store-adapter-tests"

WAIT_FOR_AEROSPIKE_TIMEOUT = 30

AEROSPIKE_CONTAINER_PORT = 3000


async def ping_aerospike(host: str, port: int) -> bool:
    try:
        import aerospike  # pyright: ignore[reportMissingImports]

        config = {"hosts": [(host, port)]}
        client = aerospike.client(config)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportAttributeAccessIssue]
        client.connect()  # pyright: ignore[reportUnknownMemberType]
        client.close()  # pyright: ignore[reportUnknownMemberType]
    except Exception:
        return False
    else:
        return True


class AerospikeFailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestAerospikeStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session")
    def aerospike_container(self) -> Generator[DockerContainer, None, None]:
        container = DockerContainer(image="aerospike/aerospike-server:latest")
        container.with_exposed_ports(AEROSPIKE_CONTAINER_PORT)
        try:
            container.start()
            # Wait for Aerospike to be ready
            wait_for_logs(container, "service ready: soon there will be cake!", timeout=60)
            yield container
        finally:
            container.stop()

    @pytest.fixture(scope="session")
    def aerospike_host(self, aerospike_container: DockerContainer) -> str:
        return aerospike_container.get_container_host_ip()

    @pytest.fixture(scope="session")
    def aerospike_port(self, aerospike_container: DockerContainer) -> int:
        return int(aerospike_container.get_exposed_port(AEROSPIKE_CONTAINER_PORT))

    @pytest.fixture(autouse=True, scope="session")
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
    async def store(self, setup_aerospike: None, aerospike_host: str, aerospike_port: int) -> AerospikeStore:
        import aerospike  # pyright: ignore[reportMissingImports]

        config = {"hosts": [(aerospike_host, aerospike_port)]}
        client = aerospike.client(config)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportAttributeAccessIssue]
        client.connect()  # pyright: ignore[reportUnknownMemberType]

        store = AerospikeStore(client=client, namespace=AEROSPIKE_NAMESPACE, set_name=AEROSPIKE_SET)  # pyright: ignore[reportUnknownArgumentType]

        # Clean up the set before tests
        with contextlib.suppress(Exception):
            client.truncate(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, 0)  # pyright: ignore[reportUnknownMemberType]

        return store

    @pytest.fixture
    async def aerospike_store(self, store: AerospikeStore) -> AerospikeStore:
        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
