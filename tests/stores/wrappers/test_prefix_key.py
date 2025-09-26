import pytest
from typing_extensions import override

from kv_store_adapter.stores.memory.store import MemoryStore
from kv_store_adapter.wrappers.prefix_keys import PrefixKeysWrapper
from tests.stores.conftest import BaseStoreTests


class TestPrefixKeyWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> PrefixKeysWrapper:
        return PrefixKeysWrapper(store=memory_store, prefix="key_prefix")
