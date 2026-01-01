import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.statistics import StatisticsWrapper
from key_value.shared.constants import DEFAULT_COLLECTION_NAME
from tests.stores.base import BaseStoreTests


class TestStatisticsWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> StatisticsWrapper:
        return StatisticsWrapper(key_value=memory_store)

    async def test_statistics_track_get_hits(self, memory_store: MemoryStore) -> None:
        """Test that get hits are tracked in statistics."""
        wrapper = StatisticsWrapper(key_value=memory_store)

        # Put a value
        await wrapper.put(collection="test", key="key", value={"data": "value"})

        # Get it multiple times
        await wrapper.get(collection="test", key="key")
        await wrapper.get(collection="test", key="key")

        # Check statistics
        stats = wrapper.statistics.get_collection("test").get
        assert stats.hit == 2
        assert stats.miss == 0
        assert stats.count == 2

    async def test_statistics_track_get_misses(self, memory_store: MemoryStore) -> None:
        """Test that get misses are tracked in statistics."""
        wrapper = StatisticsWrapper(key_value=memory_store)

        # Try to get non-existent values
        await wrapper.get(collection="test", key="missing1")
        await wrapper.get(collection="test", key="missing2")

        # Check statistics
        stats = wrapper.statistics.get_collection("test").get
        assert stats.hit == 0
        assert stats.miss == 2
        assert stats.count == 2

    async def test_statistics_track_get_many_hits_and_misses(self, memory_store: MemoryStore) -> None:
        """Test that get_many correctly tracks hits and misses."""
        wrapper = StatisticsWrapper(key_value=memory_store)

        # Put some values
        await wrapper.put(collection="test", key="key1", value={"v": 1})
        await wrapper.put(collection="test", key="key2", value={"v": 2})

        # Get many with mix of hits and misses
        results = await wrapper.get_many(collection="test", keys=["key1", "key2", "missing"])
        assert len(results) == 3
        assert results[0] is not None
        assert results[1] is not None
        assert results[2] is None

        # Check statistics
        stats = wrapper.statistics.get_collection("test").get
        assert stats.hit == 2
        assert stats.miss == 1
        assert stats.count == 3

    async def test_statistics_track_put_operations(self, memory_store: MemoryStore) -> None:
        """Test that put operations are tracked."""
        wrapper = StatisticsWrapper(key_value=memory_store)

        # Put values
        await wrapper.put(collection="test", key="key1", value={"v": 1})
        await wrapper.put(collection="test", key="key2", value={"v": 2})

        # Check statistics
        stats = wrapper.statistics.get_collection("test").put
        assert stats.count == 2

    async def test_statistics_track_put_many_operations(self, memory_store: MemoryStore) -> None:
        """Test that put_many operations are tracked."""
        wrapper = StatisticsWrapper(key_value=memory_store)

        # Put many values
        await wrapper.put_many(collection="test", keys=["k1", "k2", "k3"], values=[{"v": 1}, {"v": 2}, {"v": 3}])

        # Check statistics
        stats = wrapper.statistics.get_collection("test").put
        assert stats.count == 3

    async def test_statistics_track_delete_hits_and_misses(self, memory_store: MemoryStore) -> None:
        """Test that delete operations track hits and misses."""
        wrapper = StatisticsWrapper(key_value=memory_store)

        # Put a value
        await wrapper.put(collection="test", key="key", value={"v": 1})

        # Delete existing (hit) and non-existing (miss)
        result1 = await wrapper.delete(collection="test", key="key")
        result2 = await wrapper.delete(collection="test", key="missing")

        assert result1 is True
        assert result2 is False

        # Check statistics
        stats = wrapper.statistics.get_collection("test").delete
        assert stats.hit == 1  # One successful delete
        assert stats.miss == 1  # One failed delete
        assert stats.count == 2

    async def test_statistics_track_delete_many(self, memory_store: MemoryStore) -> None:
        """Test that delete_many correctly tracks hits and misses."""
        wrapper = StatisticsWrapper(key_value=memory_store)

        # Put some values
        await wrapper.put_many(collection="test", keys=["k1", "k2"], values=[{"v": 1}, {"v": 2}])

        # Delete with some hits and some misses
        deleted = await wrapper.delete_many(collection="test", keys=["k1", "k2", "missing"])

        assert deleted == 2

        # Check statistics
        stats = wrapper.statistics.get_collection("test").delete
        assert stats.hit == 2  # Two successful deletes
        assert stats.miss == 1  # One failed delete
        assert stats.count == 3

    async def test_statistics_track_ttl_hits_and_misses(self, memory_store: MemoryStore) -> None:
        """Test that ttl operations track hits and misses."""
        wrapper = StatisticsWrapper(key_value=memory_store)

        # Put a value
        await wrapper.put(collection="test", key="key", value={"v": 1})

        # Query ttl for existing and non-existing
        result1, ttl1 = await wrapper.ttl(collection="test", key="key")
        result2, ttl2 = await wrapper.ttl(collection="test", key="missing")

        assert result1 is not None
        assert result2 is None

        # Check statistics
        stats = wrapper.statistics.get_collection("test").ttl
        assert stats.hit == 1
        assert stats.miss == 1
        assert stats.count == 2

    async def test_statistics_track_ttl_many(self, memory_store: MemoryStore) -> None:
        """Test that ttl_many correctly tracks hits and misses."""
        wrapper = StatisticsWrapper(key_value=memory_store)

        # Put some values
        await wrapper.put_many(collection="test", keys=["k1", "k2"], values=[{"v": 1}, {"v": 2}])

        # Query ttl with mix of hits and misses
        results = await wrapper.ttl_many(collection="test", keys=["k1", "k2", "missing"])

        # Check statistics
        stats = wrapper.statistics.get_collection("test").ttl
        assert stats.hit == 2
        assert stats.miss == 1
        assert stats.count == 3

    async def test_statistics_separate_by_collection(self, memory_store: MemoryStore) -> None:
        """Test that statistics are separated by collection."""
        wrapper = StatisticsWrapper(key_value=memory_store)

        # Put and get in different collections
        await wrapper.put(collection="col1", key="key", value={"v": 1})
        await wrapper.put(collection="col2", key="key", value={"v": 2})

        # Get from both
        await wrapper.get(collection="col1", key="key")
        await wrapper.get(collection="col2", key="key")

        # Check statistics are separate
        stats1 = wrapper.statistics.get_collection("col1").get
        stats2 = wrapper.statistics.get_collection("col2").get
        assert stats1.hit == 1
        assert stats2.hit == 1
        assert stats1.count == 1
        assert stats2.count == 1

    async def test_statistics_default_collection(self, memory_store: MemoryStore) -> None:
        """Test that None collection defaults to DEFAULT_COLLECTION_NAME."""
        wrapper = StatisticsWrapper(key_value=memory_store)

        # Put and get with None collection
        await wrapper.put(collection=None, key="key", value={"v": 1})
        await wrapper.get(collection=None, key="key")

        # Statistics should be under DEFAULT_COLLECTION_NAME
        stats = wrapper.statistics.get_collection(DEFAULT_COLLECTION_NAME).get
        assert stats.hit == 1
        assert stats.count == 1

    async def test_statistics_accumulate_across_operations(self, memory_store: MemoryStore) -> None:
        """Test that statistics accumulate correctly across multiple operations."""
        wrapper = StatisticsWrapper(key_value=memory_store)

        # Perform multiple operations
        await wrapper.put(collection="test", key="k1", value={"v": 1})
        await wrapper.put(collection="test", key="k2", value={"v": 2})
        await wrapper.get(collection="test", key="k1")  # hit
        await wrapper.get(collection="test", key="k3")  # miss
        await wrapper.delete(collection="test", key="k1")  # hit
        await wrapper.delete(collection="test", key="k4")  # miss

        # Check all statistics
        col_stats = wrapper.statistics.get_collection("test")
        assert col_stats.put.count == 2
        assert col_stats.get.hit == 1
        assert col_stats.get.miss == 1
        assert col_stats.delete.hit == 1
        assert col_stats.delete.miss == 1
