import contextlib
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
class TestWindowsRegistryStore(BaseStoreTests):
    def cleanup(self):
        from winreg import HKEY_CURRENT_USER, DeleteKey

        with contextlib.suppress(Exception):
            DeleteKey(HKEY_CURRENT_USER, TEST_REGISTRY_PATH)

    @override
    @pytest.fixture
    async def store(self) -> "WindowsRegistryStore":
        from key_value.aio.stores.windows_registry.store import WindowsRegistryStore

        self.cleanup()

        return WindowsRegistryStore(registry_path=TEST_REGISTRY_PATH, hive="HKEY_CURRENT_USER")

    @override
    @pytest.mark.skip(reason="We do not test boundedness of registry stores")
    async def test_not_unbounded(self, store: BaseStore): ...
