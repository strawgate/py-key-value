import pytest
from typing_extensions import override

from kv_store_adapter.stores.simple.json_store import SimpleJSONStore
from tests.stores.conftest import BaseStoreTests


class TestSimpleJSONStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> SimpleJSONStore:
        return SimpleJSONStore()
