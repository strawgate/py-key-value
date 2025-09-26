import pytest
from typing_extensions import override

from kv_store_adapter.stores.memory.store import MemoryStore
from kv_store_adapter.wrappers.prefix_collections import PrefixCollectionsWrapper
from tests.stores.conftest import BaseStoreTests


class TestPrefixCollectionWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> PrefixCollectionsWrapper:
        memory_store: MemoryStore = MemoryStore()
        return PrefixCollectionsWrapper(store=memory_store, prefix="collection_prefix")
