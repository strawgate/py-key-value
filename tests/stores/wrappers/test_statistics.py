import pytest
from typing_extensions import override

from kv_store_adapter.stores.memory.store import MemoryStore
from kv_store_adapter.wrappers.statistics import StatisticsWrapper
from tests.stores.wrappers.conftest import BaseProtocolTests


class TestStatisticsWrapper(BaseProtocolTests):
    @override
    @pytest.fixture
    async def store(self) -> StatisticsWrapper:
        memory_store: MemoryStore = MemoryStore()
        return StatisticsWrapper(store=memory_store, track_statistics=True)

    async def test_statistics_tracking(self):
        memory_store: MemoryStore = MemoryStore()
        stats_wrapper = StatisticsWrapper(store=memory_store, track_statistics=True)

        # Initially no statistics
        assert stats_wrapper.statistics is not None
        assert len(stats_wrapper.statistics.collections) == 0

        # Test GET miss
        result = await stats_wrapper.get(collection="test", key="key1")
        assert result is None
        
        collection_stats = stats_wrapper.statistics.get_collection("test")
        assert collection_stats.get.count == 1
        assert collection_stats.get.miss == 1
        assert collection_stats.get.hit == 0

        # Test PUT
        await stats_wrapper.put(collection="test", key="key1", value={"data": "value1"})
        assert collection_stats.set.count == 1

        # Test GET hit
        result = await stats_wrapper.get(collection="test", key="key1")
        assert result == {"data": "value1"}
        assert collection_stats.get.count == 2
        assert collection_stats.get.miss == 1
        assert collection_stats.get.hit == 1

        # Test EXISTS hit
        exists = await stats_wrapper.exists(collection="test", key="key1")
        assert exists is True
        assert collection_stats.exists.count == 1
        assert collection_stats.exists.hit == 1
        assert collection_stats.exists.miss == 0

        # Test DELETE hit
        deleted = await stats_wrapper.delete(collection="test", key="key1")
        assert deleted is True
        assert collection_stats.delete.count == 1
        assert collection_stats.delete.hit == 1
        assert collection_stats.delete.miss == 0

        # Test EXISTS miss
        exists = await stats_wrapper.exists(collection="test", key="key1")
        assert exists is False
        assert collection_stats.exists.count == 2
        assert collection_stats.exists.hit == 1
        assert collection_stats.exists.miss == 1

        # Test DELETE miss
        deleted = await stats_wrapper.delete(collection="test", key="key1")
        assert deleted is False
        assert collection_stats.delete.count == 2
        assert collection_stats.delete.hit == 1
        assert collection_stats.delete.miss == 1

    async def test_statistics_disabled(self):
        memory_store: MemoryStore = MemoryStore()
        stats_wrapper = StatisticsWrapper(store=memory_store, track_statistics=False)

        assert stats_wrapper.statistics is None

        # Operations should still work
        await stats_wrapper.put(collection="test", key="key1", value={"data": "value1"})
        result = await stats_wrapper.get(collection="test", key="key1")
        assert result == {"data": "value1"}