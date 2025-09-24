import pytest
from typing_extensions import override

from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.stores.memory.store import MemoryStore
from kv_store_adapter.stores.wrappers.single_collection import SingleCollectionWrapper
from tests.stores.conftest import BaseStoreTests


class TestSingleCollectionWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> SingleCollectionWrapper:
        memory_store: MemoryStore = MemoryStore()
        return SingleCollectionWrapper(store=memory_store, collection="test")

    @pytest.mark.skip(reason="SingleCollectionWrapper does not support collection operations")
    @override
    async def test_empty_clear_collection(self, store: BaseKVStore): ...

    @pytest.mark.skip(reason="SingleCollectionWrapper does not support collection operations")
    @override
    async def test_empty_list_collections(self, store: BaseKVStore): ...

    @pytest.mark.skip(reason="SingleCollectionWrapper does not support collection operations")
    @override
    async def test_list_collections(self, store: BaseKVStore): ...

    @pytest.mark.skip(reason="SingleCollectionWrapper does not support collection operations")
    @override
    async def test_set_set_list_collections(self, store: BaseKVStore): ...
