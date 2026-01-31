"""Tests for PostgreSQL store."""

import contextlib
from collections.abc import Generator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.postgresql import PostgreSQLStore
from key_value.aio.utils.wait import async_wait_for_true
from tests.conftest import should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

try:
    import asyncpg
except ImportError:
    asyncpg = None  # type: ignore[assignment]

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
        container.waiting_for(LogMessageWaitStrategy("database system is ready to accept connections"))
        with container:
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
        # Clean up the database before each test by dropping the table
        # The table will be recreated when the store is used via _setup()
        pool = await asyncpg.create_pool(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportOptionalMemberAccess]
            host=postgresql_host,
            port=postgresql_port,
            user=POSTGRESQL_USER,
            password=POSTGRESQL_PASSWORD,
            database=POSTGRESQL_TEST_DB,
        )
        async with pool.acquire() as conn:  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            with contextlib.suppress(Exception):
                await conn.execute("DROP TABLE IF EXISTS kv_store")  # pyright: ignore[reportUnknownMemberType]
        await pool.close()  # pyright: ignore[reportUnknownMemberType]

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
