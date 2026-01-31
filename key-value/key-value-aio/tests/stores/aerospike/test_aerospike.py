import contextlib
import sys
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

import pytest
from key_value.shared.stores.wait import async_wait_for_true
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from tests.conftest import docker_container, should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

if TYPE_CHECKING:
    from key_value.aio.stores.aerospike import AerospikeStore

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="Aerospike is not supported on Windows")

# Aerospike test configuration
AEROSPIKE_HOST = "localhost"
AEROSPIKE_PORT = 3000
AEROSPIKE_NAMESPACE = "test"
AEROSPIKE_SET = "kv-store-adapter-tests"

WAIT_FOR_AEROSPIKE_TIMEOUT = 30


async def ping_aerospike() -> bool:
    try:
        import aerospike  # pyright: ignore[reportMissingImports]

        config = {"hosts": [(AEROSPIKE_HOST, AEROSPIKE_PORT)]}
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
    async def setup_aerospike(self) -> AsyncGenerator[None, None]:
        # DEFAULT_TTL must be non-zero to allow TTL writes (0 rejects TTL writes with FORBIDDEN error)
        # NSUP_PERIOD enables TTL expiration (namespace supervisor runs every N seconds)
        environment = {"DEFAULT_TTL": "86400", "NSUP_PERIOD": "1"}
        with docker_container("aerospike-test", "aerospike/aerospike-server:latest", {"3000": 3000}, environment=environment):
            if not await async_wait_for_true(bool_fn=ping_aerospike, tries=30, wait_time=1):
                msg = "Aerospike failed to start"
                raise AerospikeFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_aerospike: None) -> AsyncGenerator["AerospikeStore", None]:
        import aerospike  # pyright: ignore[reportMissingImports]

        from key_value.aio.stores.aerospike import AerospikeStore

        config = {"hosts": [(AEROSPIKE_HOST, AEROSPIKE_PORT)]}
        client = aerospike.client(config)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportAttributeAccessIssue]
        client.connect()  # pyright: ignore[reportUnknownMemberType]

        # Use a unique set name per test worker to avoid conflicts with parallel execution
        import uuid

        unique_set = f"{AEROSPIKE_SET}-{uuid.uuid4().hex[:8]}"
        store = AerospikeStore(client=client, namespace=AEROSPIKE_NAMESPACE, set_name=unique_set)  # pyright: ignore[reportUnknownArgumentType]

        yield store

        # Clean up the set after tests
        with contextlib.suppress(Exception):
            client.truncate(AEROSPIKE_NAMESPACE, unique_set, 0)  # pyright: ignore[reportUnknownMemberType]
        client.close()  # pyright: ignore[reportUnknownMemberType]

    @pytest.fixture
    async def aerospike_store(self, store: "AerospikeStore") -> "AerospikeStore":
        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
