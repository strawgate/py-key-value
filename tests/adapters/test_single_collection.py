import pytest

from kv_store_adapter.adapters.single_collection import SingleCollectionAdapter
from kv_store_adapter.stores.memory.store import MemoryStore


class TestSingleCollectionAdapter:
    @pytest.fixture
    async def adapter(self) -> SingleCollectionAdapter:
        memory_store: MemoryStore = MemoryStore()
        return SingleCollectionAdapter(store=memory_store, collection="test")

    async def test_get(self, adapter: SingleCollectionAdapter):
        assert await adapter.get(key="test") is None

    async def test_put_get(self, adapter: SingleCollectionAdapter):
        await adapter.put(key="test", value={"test": "test"})
        assert await adapter.get(key="test") == {"test": "test"}

    async def test_delete_get(self, adapter: SingleCollectionAdapter):
        _ = await adapter.delete(key="test")
        assert await adapter.get(key="test") is None

    async def test_put_exists_delete_exists(self, adapter: SingleCollectionAdapter):
        await adapter.put(key="test", value={"test": "test"})
        assert await adapter.exists(key="test")
        assert await adapter.delete(key="test")
        assert await adapter.exists(key="test") is False
