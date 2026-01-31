from typing import Any

import pytest
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.elasticsearch.store import ElasticsearchStore
from key_value.aio.stores.keyring.store import KeyringStore, KeyringV1CollectionSanitizationStrategy, KeyringV1KeySanitizationStrategy
from tests._shared_test.cases import LARGE_DATA_CASES, PositiveCases
from tests.conftest import detect_on_macos, detect_on_windows
from tests.stores.base import BaseStoreTests


class BaseTestKeychainStore(BaseStoreTests):
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


@pytest.mark.skipif(condition=not detect_on_macos(), reason="Keyrings do not support large values on MacOS")
@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestMacOSKeychainStore(BaseTestKeychainStore):
    pass


@pytest.mark.skipif(condition=not detect_on_windows(), reason="Keyrings do not support large values on Windows")
@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestWindowsKeychainStore(BaseTestKeychainStore):
    @pytest.fixture
    async def sanitizing_store(self) -> KeyringStore:
        return KeyringStore(
            service_name="py-key-value-test",
            key_sanitization_strategy=KeyringV1KeySanitizationStrategy(),
            collection_sanitization_strategy=KeyringV1CollectionSanitizationStrategy(),
        )

    @override
    async def test_long_collection_name(self, store: KeyringStore, sanitizing_store: ElasticsearchStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        with pytest.raises(Exception):  # noqa: B017, PT011
            await store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})

        await sanitizing_store.put(collection="test_collection" * 50, key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection" * 50, key="test_key") == {"test": "test"}

    @override
    async def test_long_key_name(self, store: KeyringStore, sanitizing_store: KeyringStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Tests that a long key name will not raise an error."""
        with pytest.raises(Exception):  # noqa: B017, PT011
            await store.put(collection="test_collection", key="test_key" * 100, value={"test": "test"})

        await sanitizing_store.put(collection="test_collection", key="test_key" * 100, value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection", key="test_key" * 100) == {"test": "test"}
