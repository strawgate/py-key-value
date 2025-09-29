import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.statistics import StatisticsWrapper
from tests.stores.base import BaseStoreTests


class TestStatisticsWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> StatisticsWrapper:
        return StatisticsWrapper(store=memory_store)
