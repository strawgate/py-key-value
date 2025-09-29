import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.prefix_keys import PrefixKeysWrapper
from tests.stores.base import BaseStoreTests


class TestPrefixKeyWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> PrefixKeysWrapper:
        return PrefixKeysWrapper(store=memory_store, prefix="key_prefix")
