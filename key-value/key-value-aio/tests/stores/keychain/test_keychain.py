import sys

import pytest
from typing_extensions import override

# Skip all tests in this file if not on macOS
if sys.platform != "darwin":
    pytest.skip("KeychainStore is only supported on macOS", allow_module_level=True)

from key_value.aio.stores.keychain.store import KeychainStore
from tests.stores.base import BaseStoreTests


class TestKeychainStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> KeychainStore:
        # Use a test-specific service name to avoid conflicts
        store = KeychainStore(service_name="py-key-value-test")
        await store.setup()

        return store

        # Note: Cleanup is manual - keychain entries must be removed via macOS Keychain Access
        # or by manually deleting each test key
