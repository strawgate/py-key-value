import pytest
from typing_extensions import override

from kv_store_adapter.stores.simple.store import SimpleStore
from tests.stores.conftest import BaseStoreTests


class TestSimpleStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> SimpleStore:
        return SimpleStore()
