import contextlib
from collections.abc import AsyncGenerator

import pytest
from inline_snapshot import snapshot
from key_value.shared.stores.wait import async_wait_for_true
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.cassandra import CassandraStore
from tests.conftest import docker_container, should_skip_docker_tests
from tests.stores.base import BaseStoreTests

# Cassandra test configuration
CASSANDRA_HOST = "localhost"
CASSANDRA_HOST_PORT = 9042
CASSANDRA_TEST_KEYSPACE = "kv_store_test"

WAIT_FOR_CASSANDRA_TIMEOUT = 30


async def ping_cassandra() -> bool:
    try:
        from cassandra.cluster import Cluster

        cluster = Cluster(contact_points=[CASSANDRA_HOST], port=CASSANDRA_HOST_PORT)
        session = cluster.connect()  # pyright: ignore[reportUnknownMemberType]
        session.execute("SELECT release_version FROM system.local")  # pyright: ignore[reportUnknownMemberType]
        cluster.shutdown()
    except Exception:
        return False

    return True


class CassandraFailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestCassandraStore(BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session")
    async def setup_cassandra(self) -> AsyncGenerator[None, None]:
        with docker_container("cassandra-test", "cassandra:5", {"9042": 9042}):
            if not await async_wait_for_true(bool_fn=ping_cassandra, tries=60, wait_time=2):
                msg = "Cassandra failed to start"
                raise CassandraFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_cassandra: None) -> CassandraStore:
        from cassandra.cluster import Cluster

        store = CassandraStore(contact_points=[CASSANDRA_HOST], port=CASSANDRA_HOST_PORT, keyspace=CASSANDRA_TEST_KEYSPACE)

        # Ensure a clean keyspace by dropping it if it exists
        with contextlib.suppress(Exception):
            cluster = Cluster(contact_points=[CASSANDRA_HOST], port=CASSANDRA_HOST_PORT)
            session = cluster.connect()  # pyright: ignore[reportUnknownMemberType]
            session.execute(f"DROP KEYSPACE IF EXISTS {CASSANDRA_TEST_KEYSPACE}")  # pyright: ignore[reportUnknownMemberType]
            cluster.shutdown()

        return store

    @pytest.fixture
    async def cassandra_store(self, store: CassandraStore) -> CassandraStore:
        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    async def test_cassandra_table_name_sanitization(self, cassandra_store: CassandraStore):
        """Tests that special characters in the collection name will not raise an error."""
        await cassandra_store.put(collection="test_collection!@#$%^&*()", key="test_key", value={"test": "test"})
        assert await cassandra_store.get(collection="test_collection!@#$%^&*()", key="test_key") == {"test": "test"}

        collections = await cassandra_store.collections()
        assert collections == snapshot(["test_collection_-daf4a2ec"])
