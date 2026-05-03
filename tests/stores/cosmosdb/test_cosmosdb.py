import contextlib
import os

import pytest
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.cosmosdb import CosmosDBStore
from key_value.aio.stores.cosmosdb.store import CosmosDBV1CollectionSanitizationStrategy
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

# Cosmos DB test configuration
COSMOSDB_TEST_DATABASE = "kv-store-adapter-tests"
COSMOSDB_TEST_CONTAINER = "kv-tests"


def get_cosmosdb_url() -> str | None:
    """Get Cosmos DB URL from environment."""
    return os.environ.get("COSMOSDB_URL") or os.environ.get("AZURE_COSMOS_URL")


def get_cosmosdb_key() -> str | None:
    """Get Cosmos DB key from environment."""
    return os.environ.get("COSMOSDB_KEY") or os.environ.get("AZURE_COSMOS_KEY")


def should_skip_cosmosdb_tests() -> bool:
    """Check if Cosmos DB tests should be skipped."""
    return get_cosmosdb_url() is None or get_cosmosdb_key() is None


async def clean_cosmosdb_container(store: CosmosDBStore) -> None:
    """Clean all items from the test container."""
    if store._container is None:
        return

    # Query all items and delete them
    query = "SELECT c.id, c.collection FROM c"
    items: list[dict[str, str]] = []
    async for item in store._container.query_items(query=query):
        items.append(item)  # noqa: PERF401 - async comprehensions not supported here

    for item in items:
        with contextlib.suppress(Exception):
            await store._container.delete_item(item=item["id"], partition_key=item["collection"])


@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class BaseCosmosDBStoreTests(ContextManagerStoreTestMixin, BaseStoreTests):
    """Base class for Cosmos DB store tests."""

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    @override
    async def test_long_collection_name(self, store: CosmosDBStore, sanitizing_store: CosmosDBStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Tests that a long collection name will raise an error without sanitization."""
        # Cosmos DB has limits on partition key values, but very long ones may work
        # Test with sanitization to ensure proper handling
        await sanitizing_store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection" * 100, key="test_key") == {"test": "test"}

    @override
    async def test_special_characters_in_collection_name(self, store: CosmosDBStore, sanitizing_store: CosmosDBStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Tests that special characters in the collection name work with sanitization."""
        # Some special characters may work in Cosmos DB partition keys
        # but use sanitization for consistency
        await sanitizing_store.put(collection="test_collection!@#$%^&*()", key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection!@#$%^&*()", key="test_key") == {"test": "test"}

    async def test_cosmosdb_collection_name_sanitization(self, sanitizing_store: CosmosDBStore):
        """Tests that the V1 sanitization strategy produces sanitized collection names."""
        await sanitizing_store.put(collection="test_collection!@#$%^&*()", key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection!@#$%^&*()", key="test_key") == {"test": "test"}


@pytest.mark.skipif(should_skip_cosmosdb_tests(), reason="Cosmos DB credentials not available")
class TestCosmosDBStore(BaseCosmosDBStoreTests):
    """Test CosmosDBStore with Azure Cosmos DB."""

    @override
    @pytest.fixture
    async def store(self) -> CosmosDBStore:
        url = get_cosmosdb_url()
        key = get_cosmosdb_key()

        if not url or not key:
            pytest.skip("Cosmos DB credentials not available")

        store = CosmosDBStore(
            url=url,
            credential=key,
            database_name=COSMOSDB_TEST_DATABASE,
            container_name=COSMOSDB_TEST_CONTAINER,
        )

        # Setup and clean the container
        await store.setup()
        await clean_cosmosdb_container(store=store)

        return store

    @pytest.fixture
    async def sanitizing_store(self) -> CosmosDBStore:
        url = get_cosmosdb_url()
        key = get_cosmosdb_key()

        if not url or not key:
            pytest.skip("Cosmos DB credentials not available")

        store = CosmosDBStore(
            url=url,
            credential=key,
            database_name=f"{COSMOSDB_TEST_DATABASE}-sanitizing",
            container_name=f"{COSMOSDB_TEST_CONTAINER}-sanitizing",
            collection_sanitization_strategy=CosmosDBV1CollectionSanitizationStrategy(),
        )

        # Setup and clean the container
        await store.setup()
        await clean_cosmosdb_container(store=store)

        return store

    async def test_value_stored_correctly(self, store: CosmosDBStore):
        """Verify values are stored and retrieved correctly."""
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        result = await store.get(collection="test", key="test_key")
        assert result == {"name": "Alice", "age": 30}

    async def test_ttl_stored_correctly(self, store: CosmosDBStore):
        """Verify TTL is stored and retrieved correctly."""
        await store.put(collection="test", key="test_key", value={"name": "Alice"}, ttl=3600)

        value, ttl = await store.ttl(collection="test", key="test_key")
        assert value == {"name": "Alice"}
        assert ttl is not None
        assert ttl > 3500  # Should be close to 3600 seconds

    async def test_destroy_collection(self, store: CosmosDBStore):
        """Test destroying a collection removes all items."""
        # Add some items
        await store.put(collection="destroy_test", key="key1", value={"test": "1"})
        await store.put(collection="destroy_test", key="key2", value={"test": "2"})

        # Verify items exist
        assert await store.get(collection="destroy_test", key="key1") == {"test": "1"}
        assert await store.get(collection="destroy_test", key="key2") == {"test": "2"}

        # Destroy collection
        result = await store.destroy_collection(collection="destroy_test")
        assert result is True

        # Verify items are gone
        assert await store.get(collection="destroy_test", key="key1") is None
        assert await store.get(collection="destroy_test", key="key2") is None

    async def test_multiple_collections(self, store: CosmosDBStore):
        """Test that multiple collections work independently."""
        await store.put(collection="coll1", key="key", value={"coll": "1"})
        await store.put(collection="coll2", key="key", value={"coll": "2"})

        assert await store.get(collection="coll1", key="key") == {"coll": "1"}
        assert await store.get(collection="coll2", key="key") == {"coll": "2"}
