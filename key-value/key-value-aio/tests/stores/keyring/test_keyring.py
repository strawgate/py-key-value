from typing import Any

import pytest
from key_value.shared_test.cases import LARGE_DATA_CASES, PositiveCases
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.keyring.store import KeyringStore
from tests.conftest import detect_on_linux, detect_on_windows
from tests.stores.base import BaseStoreTests


@pytest.mark.skipif(condition=detect_on_linux(), reason="KeyringStore is not available on Linux CI")
class TestKeychainStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> KeyringStore:
        # Use a test-specific service name to avoid conflicts
        store = KeyringStore(service_name="py-key-value-test")
        await store.delete_many(collection="test", keys=["test", "test_2"])
        await store.delete_many(collection="test_collection", keys=["test_key"])

        return store

    @override
    @pytest.mark.skip(reason="We do not test boundedness of keyring stores")
    async def test_not_unbounded(self, store: BaseStore): ...

    @override
    @pytest.mark.skipif(condition=detect_on_windows(), reason="Keyrings do not support large values on Windows")
    @PositiveCases.parametrize(cases=[LARGE_DATA_CASES])
    async def test_get_large_put_get(self, store: BaseStore, data: dict[str, Any], json: str, round_trip: dict[str, Any]):
        await super().test_get_large_put_get(store, data, json, round_trip=round_trip)
