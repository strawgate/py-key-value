import pytest
from typing_extensions import override

from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.stores.simple.store import SimpleStore
from tests.stores.conftest import BaseStoreTests


class TestSimpleStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> SimpleStore:
        return SimpleStore()

    @pytest.mark.skip(reason="SimpleStore does not track TTL explicitly")
    @override
    async def test_set_ttl_get_ttl(self, store: BaseKVStore): ...
