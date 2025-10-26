from typing import TYPE_CHECKING

import pytest
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from tests.conftest import detect_on_windows
from tests.stores.base import BaseStoreTests

if TYPE_CHECKING:
    from key_value.aio.stores.windows_registry.store import WindowsRegistryStore


@pytest.mark.skipif(condition=not detect_on_windows(), reason="WindowsRegistryStore is only available on Windows")
class TestWindowsRegistryStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> "WindowsRegistryStore":
        # Use a test-specific root to avoid conflicts
        from key_value.aio.stores.windows_registry.store import WindowsRegistryStore

        store = WindowsRegistryStore(registry_path="software\\py-key-value-test", hive="HKEY_CURRENT_USER")
        await store.delete_many(collection="test", keys=["test"])
        await store.delete_many(collection="test_collection", keys=["test_key"])

        return store

    @override
    @pytest.mark.skip(reason="We do not test boundedness of registry stores")
    async def test_not_unbounded(self, store: BaseStore): ...
