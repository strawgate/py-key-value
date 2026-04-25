import asyncio
import contextlib
import json
from collections.abc import Generator
from datetime import datetime, timezone
from typing import Any

import pytest
from dirty_equals import IsDatetime
from inline_snapshot import snapshot
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from typing_extensions import override

from key_value.aio._utils.wait import async_wait_for_true
from key_value.aio.errors import StoreSetupError
from key_value.aio.stores.azure_tables import AzureTablesStore
from key_value.aio.stores.base import BaseStore
from tests.conftest import should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

# ---------------------------------------------------------------------------
# Azurite test configuration
# ---------------------------------------------------------------------------

AZURITE_TEST_TABLE = "kvstoretest"  # Azure table names: alphanumeric only

WAIT_FOR_AZURITE_TIMEOUT = 30

# Pin a known-good Azurite tag. Update when Azurite ships breaking changes.
AZURITE_VERSIONS_TO_TEST = [
    "3.32.0",
]

AZURITE_TABLE_PORT = 10002

# Azurite's well-known dev account credentials. These are public, intentionally
# non-secret, and come straight from Microsoft's Azurite docs.
AZURITE_ACCOUNT_NAME = "devstoreaccount1"
AZURITE_ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)


def _connection_string(host: str, port: int) -> str:
    """Build an Azurite connection string for the Tables endpoint."""
    return (
        "DefaultEndpointsProtocol=http;"
        f"AccountName={AZURITE_ACCOUNT_NAME};"
        f"AccountKey={AZURITE_ACCOUNT_KEY};"
        f"TableEndpoint=http://{host}:{port}/{AZURITE_ACCOUNT_NAME};"
    )


async def ping_azurite(connection_string: str) -> bool:
    """Check if Azurite Tables is responsive."""
    try:
        from azure.data.tables.aio import TableServiceClient

        async with TableServiceClient.from_connection_string(conn_str=connection_string) as service:
            # list_tables returns an async pager; iterate once to force a request.
            async for _ in service.list_tables(results_per_page=1):  # pyright: ignore[reportUnknownMemberType]
                break
    except Exception:
        return False
    else:
        return True


class AzuriteFailedToStartError(Exception):
    pass


def _entity_value_payload(entity: dict[str, Any]) -> dict[str, Any]:
    """Decode the JSON-serialized ManagedEntry stored in the Value property."""
    raw = entity.get("Value")
    assert isinstance(raw, str)
    return json.loads(raw)


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestAzureTablesStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="module", params=AZURITE_VERSIONS_TO_TEST)
    def azurite_container(self, request: pytest.FixtureRequest) -> Generator[DockerContainer, None, None]:
        version = request.param
        container = DockerContainer(image=f"mcr.microsoft.com/azure-storage/azurite:{version}")
        container.with_exposed_ports(AZURITE_TABLE_PORT)
        # Azurite logs once each service is ready; we only need Tables.
        container.waiting_for(LogMessageWaitStrategy("Azurite Table service is successfully listening"))
        # Bind to 0.0.0.0 so the container's exposed port is reachable.
        container.with_command("azurite --tableHost 0.0.0.0 --skipApiVersionCheck")
        with container:
            yield container

    @pytest.fixture(scope="module")
    def azurite_host(self, azurite_container: DockerContainer) -> str:
        return azurite_container.get_container_host_ip()

    @pytest.fixture(scope="module")
    def azurite_port(self, azurite_container: DockerContainer) -> int:
        return int(azurite_container.get_exposed_port(AZURITE_TABLE_PORT))

    @pytest.fixture(scope="module")
    def azurite_connection_string(self, azurite_host: str, azurite_port: int) -> str:
        return _connection_string(azurite_host, azurite_port)

    @pytest.fixture(autouse=True, scope="module")
    async def setup_azurite(self, azurite_container: DockerContainer, azurite_connection_string: str) -> None:
        if not await async_wait_for_true(
            bool_fn=lambda: ping_azurite(azurite_connection_string),
            tries=WAIT_FOR_AZURITE_TIMEOUT,
            wait_time=1,
        ):
            msg = "Azurite failed to start"
            raise AzuriteFailedToStartError(msg)

    async def _drop_table(self, connection_string: str, table_name: str) -> None:
        """Best-effort drop of a table, suppressing not-found errors."""
        from azure.data.tables.aio import TableServiceClient

        async with TableServiceClient.from_connection_string(conn_str=connection_string) as service:
            with contextlib.suppress(Exception):
                await service.delete_table(table_name=table_name)

    @override
    @pytest.fixture
    async def store(self, setup_azurite: None, azurite_connection_string: str) -> AzureTablesStore:
        # Wipe any previous run's table contents so each test starts clean.
        await self._drop_table(azurite_connection_string, AZURITE_TEST_TABLE)
        return AzureTablesStore(
            connection_string=azurite_connection_string,
            table_name=AZURITE_TEST_TABLE,
        )

    @pytest.fixture
    async def azure_tables_store(self, store: AzureTablesStore) -> AzureTablesStore:
        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    # -----------------------------------------------------------------
    # Store-specific tests
    # -----------------------------------------------------------------

    async def test_value_stored(self, store: AzureTablesStore, azurite_connection_string: str):
        """ManagedEntry is JSON-serialized into the Value property; ExpiresAt
        is set only when ttl is passed."""
        from azure.data.tables.aio import TableServiceClient

        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        # No-TTL case
        async with TableServiceClient.from_connection_string(conn_str=azurite_connection_string) as service:
            table = service.get_table_client(table_name=AZURITE_TEST_TABLE)
            entity: dict[str, Any] = await table.get_entity(partition_key="test", row_key="test_key")  # pyright: ignore[reportAssignmentType]

        assert _entity_value_payload(entity) == snapshot(
            {
                "collection": "test",
                "created_at": IsDatetime(iso_string=True),
                "key": "test_key",
                "value": {"age": 30, "name": "Alice"},
                "version": 1,
            }
        )
        assert "ExpiresAt" not in entity, "ExpiresAt should not be set without a TTL"

        # TTL case
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30}, ttl=10)

        async with TableServiceClient.from_connection_string(conn_str=azurite_connection_string) as service:
            table = service.get_table_client(table_name=AZURITE_TEST_TABLE)
            entity = await table.get_entity(partition_key="test", row_key="test_key")  # pyright: ignore[reportAssignmentType]

        assert _entity_value_payload(entity) == snapshot(
            {
                "collection": "test",
                "created_at": IsDatetime(iso_string=True),
                "key": "test_key",
                "value": {"age": 30, "name": "Alice"},
                "expires_at": IsDatetime(iso_string=True),
                "version": 1,
            }
        )
        expires_at_raw = entity.get("ExpiresAt")
        assert isinstance(expires_at_raw, int), "ExpiresAt should be an epoch-second integer"
        now = datetime.now(timezone.utc)
        assert expires_at_raw > now.timestamp(), "ExpiresAt should be in the future"
        assert expires_at_raw < now.timestamp() + 10 + 1, "ExpiresAt should be within the configured TTL window"

    async def test_auto_create_false_raises_when_table_missing(
        self, setup_azurite: None, azurite_connection_string: str
    ):
        """auto_create=False must error when the table doesn't exist."""
        table_name = "kvstoretestautocreatefalse"
        await self._drop_table(azurite_connection_string, table_name)

        store = AzureTablesStore(
            connection_string=azurite_connection_string,
            table_name=table_name,
            auto_create=False,
        )

        with pytest.raises(StoreSetupError):
            async with store:
                await store.put(collection="test", key="test_key", value={"message": "should not get here"})

    async def test_auto_create_true_creates_table(
        self, setup_azurite: None, azurite_connection_string: str
    ):
        """auto_create=True (default) creates the table on first use."""
        from azure.data.tables.aio import TableServiceClient

        table_name = "kvstoretestautocreatetrue"
        await self._drop_table(azurite_connection_string, table_name)

        store = AzureTablesStore(
            connection_string=azurite_connection_string,
            table_name=table_name,
        )

        async with store:
            await store.put(collection="test", key="test_key", value={"message": "autocreate"})
            assert await store.get(collection="test", key="test_key") == {"message": "autocreate"}

            async with TableServiceClient.from_connection_string(conn_str=azurite_connection_string) as service:
                tables = [t.name async for t in service.list_tables()]  # pyright: ignore[reportUnknownMemberType]
                assert table_name in tables

        await self._drop_table(azurite_connection_string, table_name)

    async def test_cull_deletes_expired(self, setup_azurite: None, azurite_connection_string: str):
        """_cull() removes entries whose ExpiresAt is in the past, leaves
        un-TTLed and non-expired entries alone."""
        table_name = "kvstoretestcull"
        await self._drop_table(azurite_connection_string, table_name)

        store = AzureTablesStore(
            connection_string=azurite_connection_string,
            table_name=table_name,
        )
        async with store:
            # No TTL — should survive cull.
            await store.put(collection="c", key="permanent", value={"v": 1})
            # Long TTL — should survive.
            await store.put(collection="c", key="long", value={"v": 2}, ttl=60)
            # Tiny TTL — should be expired by the time we cull.
            await store.put(collection="c", key="short", value={"v": 3}, ttl=1)

            # Wait past the short TTL.
            await asyncio.sleep(2)

            await store.cull()

            assert await store.get(collection="c", key="permanent") == {"v": 1}
            assert await store.get(collection="c", key="long") == {"v": 2}
            assert await store.get(collection="c", key="short") is None

        await self._drop_table(azurite_connection_string, table_name)

    async def test_conflicting_auth_args_rejected(self):
        """Constructor rejects ambiguous auth-arg combinations."""
        with pytest.raises(ValueError, match="conflicting auth arguments"):
            AzureTablesStore(
                connection_string="UseDevelopmentStorage=true",
                account_name="devstoreaccount1",
                credential=None,  # type: ignore[arg-type]
                table_name="t",
            )

    async def test_no_auth_args_rejected(self):
        """Constructor errors when no auth pattern was supplied."""
        with pytest.raises(ValueError, match="requires one of"):
            AzureTablesStore(table_name="t")
