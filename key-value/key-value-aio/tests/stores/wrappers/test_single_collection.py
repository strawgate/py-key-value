import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.single_collection import SingleCollectionWrapper
from tests.stores.base import BaseStoreTests


class TestSingleCollectionWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> SingleCollectionWrapper:
        return SingleCollectionWrapper(key_value=memory_store, single_collection="test")
