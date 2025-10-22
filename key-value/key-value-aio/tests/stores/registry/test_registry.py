from typing import TYPE_CHECKING, Any

import pytest
from key_value.shared_test.cases import LARGE_TEST_DATA_ARGNAMES, LARGE_TEST_DATA_ARGVALUES, LARGE_TEST_DATA_IDS
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from tests.conftest import detect_on_linux, detect_on_macos
from tests.stores.base import BaseStoreTests

if TYPE_CHECKING:
    from key_value.aio.stores.registry.store import RegistryStore


@pytest.mark.skipif(condition=detect_on_linux() or detect_on_macos(), reason="RegistryStore is only available on Windows")
class TestRegistryStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> "RegistryStore":
        # Use a test-specific root to avoid conflicts
        from key_value.aio.stores.registry.store import RegistryStore

        store = RegistryStore(root="py-key-value-test")
        await store.delete_many(collection="test", keys=["test"])
        await store.delete_many(collection="test_collection", keys=["test_key"])

        return store

    @override
    @pytest.mark.skip(reason="We do not test boundedness of registry stores")
    async def test_not_unbounded(self, store: BaseStore): ...

    @override
    @pytest.mark.parametrize(argnames=LARGE_TEST_DATA_ARGNAMES, argvalues=LARGE_TEST_DATA_ARGVALUES, ids=LARGE_TEST_DATA_IDS)
    async def test_get_large_put_get(self, store: BaseStore, data: dict[str, Any], json: str):
        await store.put(collection="test", key="test", value=data)
        assert await store.get(collection="test", key="test") == data
