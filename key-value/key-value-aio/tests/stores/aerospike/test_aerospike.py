import contextlib
from collections.abc import AsyncGenerator

import pytest
from key_value.shared.stores.wait import async_wait_for_true
from typing_extensions import override

from key_value.aio.stores.aerospike import AerospikeStore
from key_value.aio.stores.base import BaseStore
from tests.conftest import docker_container, should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

# Aerospike test configuration
AEROSPIKE_HOST = "localhost"
AEROSPIKE_PORT = 3000
AEROSPIKE_NAMESPACE = "test"
AEROSPIKE_SET = "kv-store-adapter-tests"

WAIT_FOR_AEROSPIKE_TIMEOUT = 30


async def ping_aerospike() -> bool:
    try:
        import aerospike

        config = {"hosts": [(AEROSPIKE_HOST, AEROSPIKE_PORT)]}
        client = aerospike.client(config)
        client.connect()
        client.close()
    except Exception:
        return False
    else:
        return True


class AerospikeFailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestAerospikeStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session")
    async def setup_aerospike(self) -> AsyncGenerator[None, None]:
        with docker_container("aerospike-test", "aerospike/aerospike-server:latest", {"3000": 3000}):
            if not await async_wait_for_true(bool_fn=ping_aerospike, tries=30, wait_time=1):
                msg = "Aerospike failed to start"
                raise AerospikeFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_aerospike: None) -> AerospikeStore:
        import aerospike

        config = {"hosts": [(AEROSPIKE_HOST, AEROSPIKE_PORT)]}
        client = aerospike.client(config)
        client.connect()

        store = AerospikeStore(client=client, namespace=AEROSPIKE_NAMESPACE, set_name=AEROSPIKE_SET)

        # Clean up the set before tests
        with contextlib.suppress(Exception):
            client.truncate(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, 0)

        return store

    @pytest.fixture
    async def aerospike_store(self, store: AerospikeStore) -> AerospikeStore:
        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
