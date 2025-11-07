from typing import TYPE_CHECKING

import pytest
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from tests.conftest import detect_on_windows
from tests.stores.base import BaseStoreTests

if TYPE_CHECKING:
    from key_value.aio.stores.windows_registry.store import WindowsRegistryStore

TEST_REGISTRY_PATH = "software\\py-key-value-test"


@pytest.mark.skipif(condition=not detect_on_windows(), reason="WindowsRegistryStore is only available on Windows")
@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestWindowsRegistryStore(BaseStoreTests):
    def cleanup(self):
        from winreg import HKEY_CURRENT_USER

        from key_value.aio.stores.windows_registry.utils import delete_sub_keys

        delete_sub_keys(hive=HKEY_CURRENT_USER, sub_key=TEST_REGISTRY_PATH)

    @override
    @pytest.fixture
    async def store(self) -> "WindowsRegistryStore":
        from key_value.aio.stores.windows_registry.store import WindowsRegistryStore

        self.cleanup()

        return WindowsRegistryStore(registry_path=TEST_REGISTRY_PATH, hive="HKEY_CURRENT_USER")

    @pytest.fixture
    async def sanitizing_store(self):
        from key_value.aio.stores.windows_registry.store import (
            WindowsRegistryStore,
            WindowsRegistryV1CollectionSanitizationStrategy,
        )

        return WindowsRegistryStore(
            registry_path=TEST_REGISTRY_PATH,
            hive="HKEY_CURRENT_USER",
            collection_sanitization_strategy=WindowsRegistryV1CollectionSanitizationStrategy(),
        )

    @override
    @pytest.mark.skip(reason="We do not test boundedness of registry stores")
    async def test_not_unbounded(self, store: BaseStore): ...

    @override
    async def test_long_collection_name(self, store: "WindowsRegistryStore", sanitizing_store: "WindowsRegistryStore"):  # pyright: ignore[reportIncompatibleMethodOverride]
        with pytest.raises(Exception):  # noqa: B017, PT011
            await store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})

        await sanitizing_store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection" * 100, key="test_key") == {"test": "test"}

    @override
    async def test_long_key_name(self, store: "WindowsRegistryStore", sanitizing_store: "WindowsRegistryStore"):  # pyright: ignore[reportIncompatibleMethodOverride]
        with pytest.raises(Exception):  # noqa: B017, PT011
            await store.put(collection="test_collection", key="test_key" * 100, value={"test": "test"})

        await sanitizing_store.put(collection="test_collection", key="test_key" * 100, value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection", key="test_key" * 100) == {"test": "test"}
