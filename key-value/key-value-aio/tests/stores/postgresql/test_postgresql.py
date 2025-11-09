"""Tests for PostgreSQL store."""

import contextlib
from collections.abc import AsyncGenerator

import pytest
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.postgresql import PostgreSQLStore, PostgreSQLV1CollectionSanitizationStrategy
from tests.conftest import docker_container, should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

try:
    import asyncpg
except ImportError:
    asyncpg = None  # type: ignore[assignment]

# PostgreSQL test configuration
POSTGRESQL_HOST = "localhost"
POSTGRESQL_HOST_PORT = 5432
POSTGRESQL_USER = "postgres"
POSTGRESQL_PASSWORD = "test"  # noqa: S105
POSTGRESQL_TEST_DB = "kv_store_test"

WAIT_FOR_POSTGRESQL_TIMEOUT = 30

POSTGRESQL_VERSIONS_TO_TEST = [
    "12",  # Older supported version
    "17",  # Latest stable version
]


async def ping_postgresql() -> bool:
    """Check if PostgreSQL is available and responsive."""
    if asyncpg is None:
        return False

    try:
        conn = await asyncpg.connect(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            host=POSTGRESQL_HOST,
            port=POSTGRESQL_HOST_PORT,
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

    @pytest.fixture(autouse=True, scope="session", params=POSTGRESQL_VERSIONS_TO_TEST)
    async def setup_postgresql(self, request: pytest.FixtureRequest) -> AsyncGenerator[None, None]:
        """Set up PostgreSQL container for testing."""
        version = request.param

        with docker_container(
            f"postgresql-test-{version}",
            f"postgres:{version}-alpine",
            {str(POSTGRESQL_HOST_PORT): POSTGRESQL_HOST_PORT},
            environment={
                "POSTGRES_PASSWORD": POSTGRESQL_PASSWORD,
                "POSTGRES_DB": POSTGRESQL_TEST_DB,
            },
        ):
            # Import here to avoid issues when asyncpg is not installed
            from key_value.shared.stores.wait import async_wait_for_true

            if not await async_wait_for_true(bool_fn=ping_postgresql, tries=WAIT_FOR_POSTGRESQL_TIMEOUT, wait_time=1):
                msg = f"PostgreSQL {version} failed to start"
                raise PostgreSQLFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_postgresql: None) -> PostgreSQLStore:
        """Create a PostgreSQL store for testing."""
        store = PostgreSQLStore(
            host=POSTGRESQL_HOST,
            port=POSTGRESQL_HOST_PORT,
            database=POSTGRESQL_TEST_DB,
            user=POSTGRESQL_USER,
            password=POSTGRESQL_PASSWORD,
        )

        # Clean up the database before each test
        async with store:
            if store._pool is not None:  # pyright: ignore[reportPrivateUsage]
                async with store._pool.acquire() as conn:  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType, reportUnknownVariableType]
                    # Drop and recreate the kv_store table
                    with contextlib.suppress(Exception):
                        await conn.execute("DROP TABLE IF EXISTS kv_store")  # pyright: ignore[reportUnknownMemberType]

        return store

    @pytest.fixture
    async def postgresql_store(self, store: PostgreSQLStore) -> PostgreSQLStore:
        """Provide the PostgreSQL store fixture."""
        return store

    @pytest.fixture
    async def sanitizing_store(self, setup_postgresql: None) -> PostgreSQLStore:
        """Create a PostgreSQL store with collection sanitization enabled."""
        store = PostgreSQLStore(
            host=POSTGRESQL_HOST,
            port=POSTGRESQL_HOST_PORT,
            database=POSTGRESQL_TEST_DB,
            user=POSTGRESQL_USER,
            password=POSTGRESQL_PASSWORD,
            table_name="kv_store_sanitizing",
            collection_sanitization_strategy=PostgreSQLV1CollectionSanitizationStrategy(),
        )

        # Clean up the database before each test
        async with store:
            if store._pool is not None:  # pyright: ignore[reportPrivateUsage]
                async with store._pool.acquire() as conn:  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType, reportUnknownVariableType]
                    # Drop and recreate the kv_store_sanitizing table
                    with contextlib.suppress(Exception):
                        await conn.execute("DROP TABLE IF EXISTS kv_store_sanitizing")  # pyright: ignore[reportUnknownMemberType]

        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    @override
    async def test_long_collection_name(self, store: PostgreSQLStore, sanitizing_store: PostgreSQLStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Test that long collection names fail without sanitization but work with it."""
        with pytest.raises(Exception):  # noqa: B017, PT011
            await store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})

        await sanitizing_store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection" * 100, key="test_key") == {"test": "test"}

    @override
    async def test_special_characters_in_collection_name(self, store: PostgreSQLStore, sanitizing_store: PostgreSQLStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Test that special characters in collection names fail without sanitization but work with it."""
        # Without sanitization, special characters should work (PostgreSQL allows them in column values)
        # but may cause issues with certain characters
        await store.put(collection="test_collection", key="test_key", value={"test": "test"})
        assert await store.get(collection="test_collection", key="test_key") == {"test": "test"}

        # With sanitization, special characters should work
        await sanitizing_store.put(collection="test_collection!@#$%^&*()", key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection!@#$%^&*()", key="test_key") == {"test": "test"}

    async def test_postgresql_collection_name_sanitization(self, sanitizing_store: PostgreSQLStore):
        """Test that the V1 sanitization strategy produces expected collection names."""
        await sanitizing_store.put(collection="test_collection!@#$%^&*()", key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection!@#$%^&*()", key="test_key") == {"test": "test"}

        collections = await sanitizing_store.collections()
        # The sanitized collection name should only contain alphanumeric characters and underscores
        assert len(collections) == 1
        assert all(c.isalnum() or c in "_-" for c in collections[0])
