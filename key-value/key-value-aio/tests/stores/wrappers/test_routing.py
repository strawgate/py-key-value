import pytest

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.routing import CollectionRoutingWrapper, RoutingWrapper


class TestRoutingWrapper:
    async def test_routing_function_basic(self):
        """Test basic routing using a custom routing function."""
        store1 = MemoryStore()
        store2 = MemoryStore()

        def route(collection: str | None) -> AsyncKeyValue | None:
            if collection == "collection1":
                return store1
            if collection == "collection2":
                return store2
            return None

        wrapper = RoutingWrapper(routing_function=route, default_store=MemoryStore())

        # Put to different collections
        await wrapper.put(key="key1", value={"data": "value1"}, collection="collection1")
        await wrapper.put(key="key2", value={"data": "value2"}, collection="collection2")

        # Verify data went to the right stores
        result1 = await store1.get(key="key1", collection="collection1")
        assert result1 == {"data": "value1"}

        result2 = await store2.get(key="key2", collection="collection2")
        assert result2 == {"data": "value2"}

        # Verify store1 doesn't have key2
        result = await store1.get(key="key2", collection="collection2")
        assert result is None

    async def test_routing_with_default_store(self):
        """Test that default store is used when routing function returns None."""
        store1 = MemoryStore()
        default_store = MemoryStore()

        def route(collection: str | None) -> AsyncKeyValue | None:
            if collection == "collection1":
                return store1
            return None

        wrapper = RoutingWrapper(routing_function=route, default_store=default_store)

        # Put to mapped collection
        await wrapper.put(key="key1", value={"data": "value1"}, collection="collection1")

        # Put to unmapped collection - should use default
        await wrapper.put(key="key2", value={"data": "value2"}, collection="unmapped")

        # Verify data went to correct stores
        result1 = await store1.get(key="key1", collection="collection1")
        assert result1 == {"data": "value1"}

        result2 = await default_store.get(key="key2", collection="unmapped")
        assert result2 == {"data": "value2"}

    async def test_routing_no_default_raises_error(self):
        """Test that ValueError is raised when no store is found and no default."""

        def route(collection: str | None) -> AsyncKeyValue | None:
            return None

        wrapper = RoutingWrapper(routing_function=route)

        # Should raise ValueError
        with pytest.raises(ValueError, match="No store found for collection"):
            await wrapper.get(key="key1", collection="unmapped")

    async def test_routing_get_many(self):
        """Test get_many operation routes correctly."""
        store1 = MemoryStore()

        def route(collection: str | None) -> AsyncKeyValue | None:
            return store1

        wrapper = RoutingWrapper(routing_function=route)

        # Put multiple values
        await wrapper.put_many(
            keys=["key1", "key2", "key3"],
            values=[{"data": "v1"}, {"data": "v2"}, {"data": "v3"}],
            collection="test",
        )

        # Get many
        results = await wrapper.get_many(keys=["key1", "key2", "key3"], collection="test")
        assert results == [{"data": "v1"}, {"data": "v2"}, {"data": "v3"}]

    async def test_routing_delete_operations(self):
        """Test delete operations route correctly."""
        store1 = MemoryStore()

        def route(collection: str | None) -> AsyncKeyValue | None:
            return store1

        wrapper = RoutingWrapper(routing_function=route)

        # Put and delete single key
        await wrapper.put(key="key1", value={"data": "value1"}, collection="test")
        result = await wrapper.delete(key="key1", collection="test")
        assert result is True

        # Verify deleted
        result = await wrapper.get(key="key1", collection="test")
        assert result is None

        # Put multiple and delete many
        await wrapper.put_many(
            keys=["key2", "key3", "key4"],
            values=[{"data": "v2"}, {"data": "v3"}, {"data": "v4"}],
            collection="test",
        )

        deleted_count = await wrapper.delete_many(keys=["key2", "key3"], collection="test")
        assert deleted_count == 2

    async def test_routing_ttl_operations(self):
        """Test TTL operations route correctly."""
        store1 = MemoryStore()

        def route(collection: str | None) -> AsyncKeyValue | None:
            return store1

        wrapper = RoutingWrapper(routing_function=route)

        # Put with TTL
        await wrapper.put(key="key1", value={"data": "value1"}, collection="test", ttl=3600)

        # Check TTL
        value, ttl = await wrapper.ttl(key="key1", collection="test")
        assert value == {"data": "value1"}
        assert ttl is not None
        assert ttl > 0

        # Put many with TTL
        await wrapper.put_many(
            keys=["key2", "key3"],
            values=[{"data": "v2"}, {"data": "v3"}],
            collection="test",
            ttl=3600,
        )

        # TTL many
        results = await wrapper.ttl_many(keys=["key2", "key3"], collection="test")
        assert len(results) == 2
        assert results[0][0] == {"data": "v2"}
        assert results[1][0] == {"data": "v3"}


class TestCollectionRoutingWrapper:
    async def test_collection_map_routing(self):
        """Test basic collection map routing."""
        store1 = MemoryStore()
        store2 = MemoryStore()
        store3 = MemoryStore()

        wrapper = CollectionRoutingWrapper(
            collection_map={
                "sessions": store1,
                "users": store2,
                "cache": store3,
            }
        )

        # Put to different collections
        await wrapper.put(key="session1", value={"data": "session_data"}, collection="sessions")
        await wrapper.put(key="user1", value={"data": "user_data"}, collection="users")
        await wrapper.put(key="cache1", value={"data": "cache_data"}, collection="cache")

        # Verify data went to correct stores
        assert await store1.get(key="session1", collection="sessions") == {"data": "session_data"}
        assert await store2.get(key="user1", collection="users") == {"data": "user_data"}
        assert await store3.get(key="cache1", collection="cache") == {"data": "cache_data"}

    async def test_collection_map_with_default(self):
        """Test collection map with default store for unmapped collections."""
        store1 = MemoryStore()
        default_store = MemoryStore()

        wrapper = CollectionRoutingWrapper(
            collection_map={
                "sessions": store1,
            },
            default_store=default_store,
        )

        # Put to mapped collection
        await wrapper.put(key="key1", value={"data": "value1"}, collection="sessions")

        # Put to unmapped collection
        await wrapper.put(key="key2", value={"data": "value2"}, collection="other")

        # Verify routing
        assert await store1.get(key="key1", collection="sessions") == {"data": "value1"}
        assert await default_store.get(key="key2", collection="other") == {"data": "value2"}

    async def test_collection_map_no_default_raises(self):
        """Test that unmapped collection raises error without default store."""
        store1 = MemoryStore()

        wrapper = CollectionRoutingWrapper(
            collection_map={
                "sessions": store1,
            }
        )

        # Should raise ValueError for unmapped collection
        with pytest.raises(ValueError, match="No store found for collection"):
            await wrapper.get(key="key1", collection="unmapped")

    async def test_collection_map_none_collection(self):
        """Test handling of None collection name."""
        store1 = MemoryStore()
        default_store = MemoryStore()

        wrapper = CollectionRoutingWrapper(
            collection_map={
                "sessions": store1,
            },
            default_store=default_store,
        )

        # None collection should use default store
        await wrapper.put(key="key1", value={"data": "value1"}, collection=None)

        # Verify it went to default store
        result = await default_store.get(key="key1", collection=None)
        assert result == {"data": "value1"}

    async def test_collection_map_isolation(self):
        """Test that collections are properly isolated to their stores."""
        redis_store = MemoryStore()  # Simulating Redis
        dynamo_store = MemoryStore()  # Simulating DynamoDB
        disk_store = MemoryStore()  # Simulating disk storage

        wrapper = CollectionRoutingWrapper(
            collection_map={
                "sessions": redis_store,
                "users": dynamo_store,
            },
            default_store=disk_store,
        )

        # Put same key to different collections
        await wrapper.put(key="id123", value={"type": "session"}, collection="sessions")
        await wrapper.put(key="id123", value={"type": "user"}, collection="users")
        await wrapper.put(key="id123", value={"type": "other"}, collection="logs")

        # Verify each went to correct store
        assert await redis_store.get(key="id123", collection="sessions") == {"type": "session"}
        assert await dynamo_store.get(key="id123", collection="users") == {"type": "user"}
        assert await disk_store.get(key="id123", collection="logs") == {"type": "other"}

        # Verify isolation - stores don't have other collections' data
        assert await redis_store.get(key="id123", collection="users") is None
        assert await dynamo_store.get(key="id123", collection="sessions") is None
