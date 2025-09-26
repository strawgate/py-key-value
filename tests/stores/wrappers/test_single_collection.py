import pytest
from typing_extensions import override

from kv_store_adapter.stores.memory.store import MemoryStore
from kv_store_adapter.wrappers.single_collection import SingleCollectionWrapper
from tests.stores.conftest import BaseStoreTests


class TestSingleCollectionWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> SingleCollectionWrapper:
        memory_store: MemoryStore = MemoryStore()
        return SingleCollectionWrapper(store=memory_store, single_collection="test", default_collection="test")
