import pytest
from typing_extensions import override

from kv_store_adapter.stores.memory.store import MemoryStore
from kv_store_adapter.stores.wrappers.prefix_collection import PrefixCollectionWrapper
from tests.stores.conftest import BaseStoreTests


class TestPrefixCollectionWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> PrefixCollectionWrapper:
        memory_store: MemoryStore = MemoryStore()
        return PrefixCollectionWrapper(store=memory_store, prefix="collection_prefix")
