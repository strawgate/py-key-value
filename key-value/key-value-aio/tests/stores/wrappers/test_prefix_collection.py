import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.prefix_collections import PrefixCollectionsWrapper
from tests.stores.base import BaseStoreTests


class TestPrefixCollectionWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> PrefixCollectionsWrapper:
        return PrefixCollectionsWrapper(key_value=memory_store, prefix="collection_prefix")
