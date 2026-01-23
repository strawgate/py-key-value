"""Tests for PostgreSQL store."""

import contextlib
from collections.abc import AsyncGenerator

import pytest
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.postgresql import PostgreSQLStore
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
        # Initialize the pool without entering context manager to avoid closing it
        await store._ensure_pool_initialized()  # pyright: ignore[reportPrivateUsage]
        if store._pool is not None:  # pyright: ignore[reportPrivateUsage]
            async with store._pool.acquire() as conn:  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType, reportUnknownVariableType]
                # Drop and recreate the kv_store table
                with contextlib.suppress(Exception):
                    await conn.execute("DROP TABLE IF EXISTS kv_store")  # pyright: ignore[reportUnknownMemberType]

        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
