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
from types_aiobotocore_dynamodb.client import DynamoDBClient
from types_aiobotocore_dynamodb.type_defs import GetItemOutputTypeDef
from typing_extensions import override

from key_value.aio._utils.wait import async_wait_for_true
from key_value.aio.errors import StoreSetupError
from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.dynamodb import DynamoDBStore
from tests.conftest import should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

# DynamoDB test configuration
DYNAMODB_TEST_TABLE = "kv-store-test"

WAIT_FOR_DYNAMODB_TIMEOUT = 30

DYNAMODB_VERSIONS_TO_TEST = [
    "2.0.0",  # Released Jul 2023
    "3.1.0",  # Released Sep 2025
]

DYNAMODB_CONTAINER_PORT = 8000


async def ping_dynamodb(endpoint_url: str) -> bool:
    """Check if DynamoDB Local is running."""
    from key_value.aio.stores.dynamodb.store import _create_dynamodb_client_context, _create_dynamodb_session

    try:
        session = _create_dynamodb_session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        async with _create_dynamodb_client_context(session, endpoint_url=endpoint_url) as client:
            await client.list_tables()
    except Exception:
        return False
    else:
        return True


class DynamoDBFailedToStartError(Exception):
    pass


def get_value_from_response(response: GetItemOutputTypeDef) -> dict[str, Any]:
    return json.loads(response.get("Item", {}).get("value", {}).get("S", {}))  # pyright: ignore[reportArgumentType]


def get_dynamo_client_from_store(store: DynamoDBStore) -> DynamoDBClient:
    return store._connected_client


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestDynamoDBStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="module", params=DYNAMODB_VERSIONS_TO_TEST)
    def dynamodb_container(self, request: pytest.FixtureRequest) -> Generator[DockerContainer, None, None]:
        version = request.param
        container = DockerContainer(image=f"amazon/dynamodb-local:{version}")
        container.with_exposed_ports(DYNAMODB_CONTAINER_PORT)
        container.waiting_for(LogMessageWaitStrategy("Initializing DynamoDB Local"))
        with container:
            yield container

    @pytest.fixture(scope="module")
    def dynamodb_host(self, dynamodb_container: DockerContainer) -> str:
        return dynamodb_container.get_container_host_ip()

    @pytest.fixture(scope="module")
    def dynamodb_port(self, dynamodb_container: DockerContainer) -> int:
        return int(dynamodb_container.get_exposed_port(DYNAMODB_CONTAINER_PORT))

    @pytest.fixture(scope="module")
    def dynamodb_endpoint(self, dynamodb_host: str, dynamodb_port: int) -> str:
        return f"http://{dynamodb_host}:{dynamodb_port}"

    @pytest.fixture(autouse=True, scope="module")
    async def setup_dynamodb(self, dynamodb_container: DockerContainer, dynamodb_endpoint: str) -> None:
        if not await async_wait_for_true(bool_fn=lambda: ping_dynamodb(dynamodb_endpoint), tries=WAIT_FOR_DYNAMODB_TIMEOUT, wait_time=1):
            msg = "DynamoDB failed to start"
            raise DynamoDBFailedToStartError(msg)

    @override
    @pytest.fixture
    async def store(self, setup_dynamodb: None, dynamodb_endpoint: str) -> DynamoDBStore:
        from key_value.aio.stores.dynamodb.store import _create_dynamodb_client_context, _create_dynamodb_session

        store = DynamoDBStore(
            table_name=DYNAMODB_TEST_TABLE,
            endpoint_url=dynamodb_endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )

        # Clean up test table if it exists
        session = _create_dynamodb_session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        async with _create_dynamodb_client_context(session, endpoint_url=dynamodb_endpoint) as client:
            with contextlib.suppress(Exception):
                await client.delete_table(TableName=DYNAMODB_TEST_TABLE)
                # Wait for table to be deleted
                waiter = client.get_waiter("table_not_exists")
                await waiter.wait(TableName=DYNAMODB_TEST_TABLE)

        return store

    @pytest.fixture
    async def dynamodb_store(self, store: DynamoDBStore) -> DynamoDBStore:
        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    async def test_value_stored(self, store: DynamoDBStore):
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        response = await get_dynamo_client_from_store(store=store).get_item(
            TableName=DYNAMODB_TEST_TABLE, Key={"collection": {"S": "test"}, "key": {"S": "test_key"}}
        )
        assert get_value_from_response(response=response) == snapshot(
            {
                "collection": "test",
                "created_at": IsDatetime(iso_string=True),
                "key": "test_key",
                "value": {"age": 30, "name": "Alice"},
                "version": 1,
            }
        )

        assert "ttl" not in response.get("Item", {})

        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30}, ttl=10)

        response = await get_dynamo_client_from_store(store=store).get_item(
            TableName=DYNAMODB_TEST_TABLE, Key={"collection": {"S": "test"}, "key": {"S": "test_key"}}
        )
        assert get_value_from_response(response=response) == snapshot(
            {
                "collection": "test",
                "created_at": IsDatetime(iso_string=True),
                "value": {"age": 30, "name": "Alice"},
                "key": "test_key",
                "expires_at": IsDatetime(iso_string=True),
                "version": 1,
            }
        )
        # Verify DynamoDB TTL attribute is set for automatic expiration
        assert "ttl" in response.get("Item", {}), "DynamoDB TTL attribute should be set when ttl parameter is provided"
        ttl_value = int(response["Item"]["ttl"]["N"])  # pyright: ignore[reportTypedDictNotRequiredAccess]
        now = datetime.now(timezone.utc)
        assert ttl_value > now.timestamp(), "TTL timestamp should be a positive integer"
        assert ttl_value < now.timestamp() + 10, "TTL timestamp should be less than the expected expiration time"

    async def test_table_config_sse_specification(self, setup_dynamodb: None, dynamodb_endpoint: str):
        """Test that SSESpecification can be passed via table_config."""
        from key_value.aio.stores.dynamodb.store import _create_dynamodb_client_context, _create_dynamodb_session

        table_name = "kv-store-test-sse"

        # Clean up table if it exists
        session = _create_dynamodb_session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        async with _create_dynamodb_client_context(session, endpoint_url=dynamodb_endpoint) as client:
            with contextlib.suppress(Exception):
                await client.delete_table(TableName=table_name)
                waiter = client.get_waiter("table_not_exists")
                await waiter.wait(TableName=table_name)

        # Create store with SSE configuration
        store = DynamoDBStore(
            table_name=table_name,
            endpoint_url=dynamodb_endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
            table_config={
                "SSESpecification": {
                    "Enabled": True,
                    "SSEType": "AES256",
                }
            },
        )

        async with store:
            # Verify table was created successfully
            async with _create_dynamodb_client_context(session, endpoint_url=dynamodb_endpoint) as client:
                table_description = await client.describe_table(TableName=table_name)

                # DynamoDB Local might not fully support SSE, but we can verify the store accepts the config
                # The important thing is that the store doesn't error when table_config is provided
                assert table_description is not None

            # Verify basic operations still work
            await store.put(collection="test", key="test_key", value={"message": "SSE test"})
            result = await store.get(collection="test", key="test_key")
            assert result == {"message": "SSE test"}

    async def test_auto_create_false_raises_error(self, setup_dynamodb: None, dynamodb_endpoint: str):
        """Test that auto_create=False raises error when table doesn't exist."""
        from key_value.aio.stores.dynamodb.store import _create_dynamodb_client_context, _create_dynamodb_session

        table_name = "kv-store-test-nonexistent"

        # Clean up table if it exists to ensure it doesn't exist
        session = _create_dynamodb_session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        async with _create_dynamodb_client_context(session, endpoint_url=dynamodb_endpoint) as client:
            with contextlib.suppress(Exception):
                await client.delete_table(TableName=table_name)
                waiter = client.get_waiter("table_not_exists")
                await waiter.wait(TableName=table_name)

        # Create store with auto_create=False
        store = DynamoDBStore(
            table_name=table_name,
            endpoint_url=dynamodb_endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
            auto_create=False,
        )

        # Attempting to use the store should raise StoreSetupError (which wraps the ValueError)
        with pytest.raises(StoreSetupError, match=f"Table '{table_name}' does not exist"):
            async with store:
                await store.put(collection="test", key="test_key", value={"message": "test"})

    async def test_auto_create_true_creates_table(self, setup_dynamodb: None, dynamodb_endpoint: str):
        """Test that auto_create=True (default) creates table when it doesn't exist."""
        from key_value.aio.stores.dynamodb.store import _create_dynamodb_client_context, _create_dynamodb_session

        table_name = "kv-store-test-autocreate"

        # Clean up table if it exists to ensure it doesn't exist
        session = _create_dynamodb_session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        async with _create_dynamodb_client_context(session, endpoint_url=dynamodb_endpoint) as client:
            with contextlib.suppress(Exception):
                await client.delete_table(TableName=table_name)
                waiter = client.get_waiter("table_not_exists")
                await waiter.wait(TableName=table_name)

        # Create store with auto_create=True (default)
        store = DynamoDBStore(
            table_name=table_name,
            endpoint_url=dynamodb_endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
            auto_create=True,
        )

        # Store should work and create table automatically
        async with store:
            await store.put(collection="test", key="test_key", value={"message": "autocreate test"})
            result = await store.get(collection="test", key="test_key")
            assert result == {"message": "autocreate test"}

            # Verify table was actually created
            async with _create_dynamodb_client_context(session, endpoint_url=dynamodb_endpoint) as client:
                table_description = await client.describe_table(TableName=table_name)
                assert table_description is not None
