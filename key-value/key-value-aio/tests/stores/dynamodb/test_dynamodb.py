import contextlib
import json
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from dirty_equals import IsDatetime
from inline_snapshot import snapshot
from key_value.shared.stores.wait import async_wait_for_true
from types_aiobotocore_dynamodb.client import DynamoDBClient
from types_aiobotocore_dynamodb.type_defs import GetItemOutputTypeDef
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.dynamodb import DynamoDBStore
from tests.conftest import docker_container, should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

# DynamoDB test configuration
DYNAMODB_HOST = "localhost"
DYNAMODB_HOST_PORT = 8000
DYNAMODB_ENDPOINT = f"http://{DYNAMODB_HOST}:{DYNAMODB_HOST_PORT}"
DYNAMODB_TEST_TABLE = "kv-store-test"

WAIT_FOR_DYNAMODB_TIMEOUT = 30

DYNAMODB_VERSIONS_TO_TEST = [
    "2.0.0",  # Released Jul 2023
    "3.1.0",  # Released Sep 2025
]

DYNAMODB_CONTAINER_PORT = 8000


async def ping_dynamodb() -> bool:
    """Check if DynamoDB Local is running."""
    try:
        import aioboto3

        session = aioboto3.Session(
            aws_access_key_id="test",
            aws_secret_access_key="test",  # noqa: S106
            region_name="us-east-1",
        )
        async with session.client(service_name="dynamodb", endpoint_url=DYNAMODB_ENDPOINT) as client:  # type: ignore
            await client.list_tables()  # type: ignore
    except Exception:
        return False
    else:
        return True


class DynamoDBFailedToStartError(Exception):
    pass


def get_value_from_response(response: GetItemOutputTypeDef) -> dict[str, Any]:
    return json.loads(response.get("Item", {}).get("value", {}).get("S", {}))  # pyright: ignore[reportArgumentType]


def get_dynamo_client_from_store(store: DynamoDBStore) -> DynamoDBClient:
    return store._connected_client  # pyright: ignore[reportPrivateUsage]


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestDynamoDBStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session", params=DYNAMODB_VERSIONS_TO_TEST)
    async def setup_dynamodb(self, request: pytest.FixtureRequest) -> AsyncGenerator[None, None]:
        version = request.param

        # DynamoDB Local container
        with docker_container(
            f"dynamodb-test-{version}",
            f"amazon/dynamodb-local:{version}",
            {str(DYNAMODB_CONTAINER_PORT): DYNAMODB_HOST_PORT},
        ):
            if not await async_wait_for_true(bool_fn=ping_dynamodb, tries=WAIT_FOR_DYNAMODB_TIMEOUT, wait_time=1):
                msg = f"DynamoDB {version} failed to start"
                raise DynamoDBFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_dynamodb: None) -> DynamoDBStore:
        store = DynamoDBStore(
            table_name=DYNAMODB_TEST_TABLE,
            endpoint_url=DYNAMODB_ENDPOINT,
            aws_access_key_id="test",
            aws_secret_access_key="test",  # noqa: S106
            region_name="us-east-1",
        )

        # Clean up test table if it exists
        import aioboto3

        session = aioboto3.Session(
            aws_access_key_id="test",
            aws_secret_access_key="test",  # noqa: S106
            region_name="us-east-1",
        )
        async with session.client(service_name="dynamodb", endpoint_url=DYNAMODB_ENDPOINT) as client:  # type: ignore
            with contextlib.suppress(Exception):
                await client.delete_table(TableName=DYNAMODB_TEST_TABLE)  # type: ignore
                # Wait for table to be deleted
                waiter = client.get_waiter("table_not_exists")  # type: ignore
                await waiter.wait(TableName=DYNAMODB_TEST_TABLE)  # type: ignore

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
            {"created_at": IsDatetime(iso_string=True), "value": {"age": 30, "name": "Alice"}}
        )

        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30}, ttl=10)

        response = await get_dynamo_client_from_store(store=store).get_item(
            TableName=DYNAMODB_TEST_TABLE, Key={"collection": {"S": "test"}, "key": {"S": "test_key"}}
        )
        assert get_value_from_response(response=response) == snapshot(
            {"created_at": IsDatetime(iso_string=True), "value": {"age": 30, "name": "Alice"}, "expires_at": IsDatetime(iso_string=True)}
        )
