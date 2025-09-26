import pytest
from typing_extensions import override

from kv_store_adapter.stores.memory.store import MemoryStore
from kv_store_adapter.wrappers.statistics import StatisticsWrapper
from tests.stores.conftest import BaseStoreTests


class TestStatisticsWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> StatisticsWrapper:
        return StatisticsWrapper(store=memory_store)
