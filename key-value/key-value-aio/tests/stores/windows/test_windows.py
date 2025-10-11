import sys

import pytest
from typing_extensions import override

from key_value.aio.stores.windows.store import WindowsStore
from tests.stores.base import BaseStoreTests

# Skip all tests in this file if not on Windows
pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="Windows-only tests")


class TestWindowsStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> WindowsStore:
        store = WindowsStore(service_name="py-key-value-test")
        # Clean up before test
        await store.setup()
        await store.destroy()
        yield store
        # Clean up after test
        await store.destroy()
