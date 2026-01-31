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
