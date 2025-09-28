import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from tests.stores.base import BaseStoreTests


class TestMemoryStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> MemoryStore:
        return MemoryStore(max_entries_per_collection=500)
