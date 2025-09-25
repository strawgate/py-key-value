import pytest
from typing_extensions import override

from kv_store_adapter.stores.memory.store import MemoryStore
from kv_store_adapter.wrappers.prefix_key import PrefixKeyWrapper
from tests.stores.wrappers.conftest import BaseProtocolTests


class TestPrefixKeyWrapper(BaseProtocolTests):
    @override
    @pytest.fixture
    async def store(self) -> PrefixKeyWrapper:
        memory_store: MemoryStore = MemoryStore()
        return PrefixKeyWrapper(store=memory_store, prefix="key_prefix")
