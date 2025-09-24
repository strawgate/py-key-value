import pytest
from typing_extensions import override

from kv_store_adapter.stores.memory.store import MemoryStore
from tests.stores.conftest import BaseStoreTests


class TestMemoryStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> MemoryStore:
        return MemoryStore()
