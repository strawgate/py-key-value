import pytest
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.keyring.store import KeyringStore
from tests.stores.base import BaseStoreTests


class TestKeychainStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> KeyringStore:
        # Use a test-specific service name to avoid conflicts
        store = KeyringStore(service_name="py-key-value-test")
        await store.delete_many(collection="test", keys=["test"])
        await store.delete_many(collection="test_collection", keys=["test_key"])

        return store

    @override
    @pytest.mark.skip(reason="We do not test boundedness of keyring stores")
    async def test_not_unbounded(self, store: BaseStore): ...
