import asyncio
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from typing_extensions import override

from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.stores.dynamodb import DynamoDBStore
from tests.stores.conftest import BaseStoreTests

# DynamoDB test configuration
TEST_TABLE_NAME = "kv-store-test"
AWS_REGION = "us-east-1"

# Skip DynamoDB tests if boto3 is not installed or credentials are not configured
boto3_available = True
try:
    import boto3
    from moto import mock_aws
except ImportError:
    boto3_available = False


@pytest.mark.skipif(not boto3_available, reason="boto3 or moto not available")
class TestDynamoDBStore(BaseStoreTests):
    @pytest.fixture(autouse=True, scope="function")
    async def setup_dynamodb(self) -> AsyncGenerator[None, None]:
        """Set up mock DynamoDB for testing."""
        with mock_aws():
            yield

    @override
    @pytest.fixture
    async def store(self, setup_dynamodb: None) -> DynamoDBStore:
        """Create a DynamoDB store for testing."""
        # Create the store with test configuration
        dynamodb_store = DynamoDBStore(
            table_name=TEST_TABLE_NAME,
            region_name=AWS_REGION,
            aws_access_key_id="test",
            aws_secret_access_key="test"
        )
        
        # Setup will create the table if it doesn't exist
        await dynamodb_store.setup()
        
        return dynamodb_store

    async def test_dynamodb_client_connection(self, setup_dynamodb: None):
        """Test DynamoDB store creation with existing client."""
        import boto3
        from moto import mock_aws
        
        with mock_aws():
            client = boto3.client(
                "dynamodb", 
                region_name=AWS_REGION,
                aws_access_key_id="test",
                aws_secret_access_key="test"
            )
            
            store = DynamoDBStore(table_name=TEST_TABLE_NAME, client=client)
            await store.setup()
            
            await store.put(collection="test", key="client_test", value={"test": "value"})
            result = await store.get(collection="test", key="client_test")
            assert result == {"test": "value"}

    async def test_dynamodb_ttl_functionality(self, store: DynamoDBStore):
        """Test DynamoDB-specific TTL functionality."""
        # Test storing with TTL
        await store.put(collection="test", key="ttl_test", value={"test": "value"}, ttl=300)
        
        # Verify the item was stored
        result = await store.get(collection="test", key="ttl_test")
        assert result == {"test": "value"}
        
        # Verify TTL information
        ttl_info = await store.ttl(collection="test", key="ttl_test")
        assert ttl_info is not None
        assert ttl_info.ttl == 300

    async def test_dynamodb_collection_operations(self, store: DynamoDBStore):
        """Test collection-specific operations."""
        # Add items to different collections
        await store.put(collection="collection1", key="key1", value={"data": "value1"})
        await store.put(collection="collection1", key="key2", value={"data": "value2"})
        await store.put(collection="collection2", key="key1", value={"data": "value3"})
        
        # Test keys in collection
        keys1 = await store.keys("collection1")
        assert set(keys1) == {"key1", "key2"}
        
        keys2 = await store.keys("collection2")
        assert keys2 == ["key1"]
        
        # Test list collections
        collections = await store.list_collections()
        assert set(collections) == {"collection1", "collection2"}
        
        # Test clear collection
        deleted_count = await store.clear_collection("collection1")
        assert deleted_count == 2
        
        # Verify collection1 is now empty
        keys1_after = await store.keys("collection1")
        assert keys1_after == []
        
        # Verify collection2 is unchanged
        keys2_after = await store.keys("collection2")
        assert keys2_after == ["key1"]

    @pytest.mark.skip(reason="Distributed cloud stores are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseKVStore): ...


# Test case for when boto3 is not available
@pytest.mark.skipif(boto3_available, reason="boto3 is available")
def test_dynamodb_import_error():
    """Test that proper error is raised when boto3 is not available."""
    with pytest.raises(ImportError, match="boto3 is required for DynamoDBStore"):
        from kv_store_adapter.stores.dynamodb import DynamoDBStore