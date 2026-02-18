import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from tests.stores.base import BaseStoreTests


class TestMemoryStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> MemoryStore:
        return MemoryStore(max_entries_per_collection=500)

    async def test_seed(self):
        store = MemoryStore(max_entries_per_collection=500, seed={"test_collection": {"test_key": {"obj_key": "obj_value"}}})
        assert await store.get(key="test_key", collection="test_collection") == {"obj_key": "obj_value"}

    async def test_keys_limit_zero(self, store: MemoryStore):
        await store.put(collection="test", key="k1", value={"a": "b"})
        assert await store.keys(collection="test", limit=0) == []

    async def test_collections_limit_zero(self, store: MemoryStore):
        await store.put(collection="test", key="k1", value={"a": "b"})
        assert await store.collections(limit=0) == []
