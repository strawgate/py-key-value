"""Tests for PostgreSQL store."""

import contextlib
from collections.abc import Generator

import pytest
from testcontainers.core.container import DockerContainer
from typing_extensions import override

from key_value.aio._utils.wait import async_wait_for_true
from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.postgresql import PostgreSQLStore
from tests.conftest import run_container_with_log_wait, should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

try:
    import asyncpg
except ImportError:
    asyncpg = None

# PostgreSQL test configuration
POSTGRESQL_USER = "postgres"
POSTGRESQL_PASSWORD = "test"
POSTGRESQL_TEST_DB = "kv_store_test"

WAIT_FOR_POSTGRESQL_TIMEOUT = 30

POSTGRESQL_VERSIONS_TO_TEST = [
    "12",  # Older supported version
    "17",  # Latest stable version
]

POSTGRESQL_CONTAINER_PORT = 5432


async def ping_postgresql(host: str, port: int) -> bool:
    """Check if PostgreSQL is available and responsive."""
    if asyncpg is None:
        return False

    try:
        conn = await asyncpg.connect(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            host=host,
            port=port,
            user=POSTGRESQL_USER,
            password=POSTGRESQL_PASSWORD,
            database="postgres",
        )
        await conn.close()  # pyright: ignore[reportUnknownMemberType]
    except Exception:
        return False
    else:
        return True


class PostgreSQLFailedToStartError(Exception):
    """Raised when PostgreSQL fails to start in tests."""


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestPostgreSQLStore(ContextManagerStoreTestMixin, BaseStoreTests):
    """Test suite for PostgreSQL store."""

    @pytest.fixture(autouse=True, scope="module", params=POSTGRESQL_VERSIONS_TO_TEST)
    def postgresql_container(self, request: pytest.FixtureRequest) -> Generator[DockerContainer, None, None]:
        """Set up PostgreSQL container for testing."""
        version = request.param
        container = DockerContainer(image=f"postgres:{version}-alpine")
        container.with_exposed_ports(POSTGRESQL_CONTAINER_PORT)
        container.with_env("POSTGRES_PASSWORD", POSTGRESQL_PASSWORD)
        container.with_env("POSTGRES_DB", POSTGRESQL_TEST_DB)
        with run_container_with_log_wait(container, "database system is ready to accept connections"):
            yield container

    @pytest.fixture(scope="module")
    def postgresql_host(self, postgresql_container: DockerContainer) -> str:
        return postgresql_container.get_container_host_ip()

    @pytest.fixture(scope="module")
    def postgresql_port(self, postgresql_container: DockerContainer) -> int:
        return int(postgresql_container.get_exposed_port(POSTGRESQL_CONTAINER_PORT))

    @pytest.fixture(autouse=True, scope="module")
    async def setup_postgresql(self, postgresql_container: DockerContainer, postgresql_host: str, postgresql_port: int) -> None:
        """Wait for PostgreSQL to be ready."""

        async def _ping() -> bool:
            return await ping_postgresql(postgresql_host, postgresql_port)

        if not await async_wait_for_true(bool_fn=_ping, tries=WAIT_FOR_POSTGRESQL_TIMEOUT, wait_time=1):
            msg = "PostgreSQL failed to start"
            raise PostgreSQLFailedToStartError(msg)

    @override
    @pytest.fixture
    async def store(self, setup_postgresql: None, postgresql_host: str, postgresql_port: int) -> PostgreSQLStore:
        """Create a PostgreSQL store for testing."""
        from key_value.aio.stores.postgresql.store import _create_postgresql_pool

        # Clean up the database before each test by dropping the table
        # The table will be recreated when the store is used via _setup()
        pool = await _create_postgresql_pool(
            host=postgresql_host,
            port=postgresql_port,
            user=POSTGRESQL_USER,
            password=POSTGRESQL_PASSWORD,
            database=POSTGRESQL_TEST_DB,
        )
        async with pool.acquire() as conn:  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            with contextlib.suppress(Exception):
                await conn.execute("DROP TABLE IF EXISTS kv_store")  # pyright: ignore[reportUnknownMemberType]
        await pool.close()

        return PostgreSQLStore(
            host=postgresql_host,
            port=postgresql_port,
            database=POSTGRESQL_TEST_DB,
            user=POSTGRESQL_USER,
            password=POSTGRESQL_PASSWORD,
        )

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    async def test_keys_empty(self, store: PostgreSQLStore):
        """keys() on an empty collection returns an empty list."""
        assert await store.keys(collection="test_collection") == []

    async def test_keys_after_put(self, store: PostgreSQLStore):
        """keys() returns keys that were put into the collection."""
        await store.put(collection="test_collection", key="alpha", value={"v": 1})
        await store.put(collection="test_collection", key="beta", value={"v": 2})
        await store.put(collection="test_collection", key="gamma", value={"v": 3})

        # _get_collection_keys sorts by key via ORDER BY.
        assert await store.keys(collection="test_collection") == ["alpha", "beta", "gamma"]

    async def test_keys_are_collection_scoped(self, store: PostgreSQLStore):
        """keys() only returns keys from the requested collection."""
        await store.put(collection="collection_a", key="a_key", value={"v": 1})
        await store.put(collection="collection_b", key="b_key", value={"v": 2})

        assert await store.keys(collection="collection_a") == ["a_key"]
        assert await store.keys(collection="collection_b") == ["b_key"]

    async def test_keys_respects_limit(self, store: PostgreSQLStore):
        """keys() honors the limit parameter."""
        for i in range(5):
            await store.put(collection="test_collection", key=f"key_{i}", value={"v": i})

        limited = await store.keys(collection="test_collection", limit=2)
        assert len(limited) == 2
        # With ORDER BY key, the first two are key_0 and key_1.
        assert limited == ["key_0", "key_1"]

    async def test_keys_after_delete(self, store: PostgreSQLStore):
        """keys() reflects deletes."""
        await store.put(collection="test_collection", key="keep", value={"v": 1})
        await store.put(collection="test_collection", key="drop", value={"v": 2})
        await store.delete(collection="test_collection", key="drop")

        assert await store.keys(collection="test_collection") == ["keep"]
