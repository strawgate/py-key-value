import contextlib
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from dirty_equals import IsFloat
from inline_snapshot import snapshot
from key_value.shared.stores.wait import async_wait_for_true
from key_value.shared.utils.managed_entry import ManagedEntry
from pymongo import AsyncMongoClient
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.mongodb import MongoDBStore
from key_value.aio.stores.mongodb.store import document_to_managed_entry, managed_entry_to_document
from tests.conftest import docker_container, should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

# MongoDB test configuration
MONGODB_HOST = "localhost"
MONGODB_HOST_PORT = 27017
MONGODB_TEST_DB = "kv-store-adapter-tests"

WAIT_FOR_MONGODB_TIMEOUT = 30

MONGODB_VERSIONS_TO_TEST = [
    "5.0",  # Older supported version
    "8.0",  # Latest stable version
]


async def ping_mongodb() -> bool:
    try:
        client: AsyncMongoClient[Any] = AsyncMongoClient[Any](host=MONGODB_HOST, port=MONGODB_HOST_PORT)
        _ = await client.list_database_names()
    except Exception:
        return False

    return True


class MongoDBFailedToStartError(Exception):
    pass


def test_managed_entry_document_conversion_native_mode():
    created_at = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    expires_at = created_at + timedelta(seconds=10)

    managed_entry = ManagedEntry(value={"test": "test"}, created_at=created_at, expires_at=expires_at)
    document = managed_entry_to_document(key="test", managed_entry=managed_entry, native_storage=True)

    assert document == snapshot(
        {
            "key": "test",
            "value": {"object": {"test": "test"}},
            "created_at": datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
            "expires_at": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
        }
    )

    round_trip_managed_entry = document_to_managed_entry(document=document)

    assert round_trip_managed_entry.value == managed_entry.value
    assert round_trip_managed_entry.created_at == created_at
    assert round_trip_managed_entry.ttl == IsFloat(lt=0)
    assert round_trip_managed_entry.expires_at == expires_at


def test_managed_entry_document_conversion_legacy_mode():
    created_at = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    expires_at = created_at + timedelta(seconds=10)

    managed_entry = ManagedEntry(value={"test": "test"}, created_at=created_at, expires_at=expires_at)
    document = managed_entry_to_document(key="test", managed_entry=managed_entry, native_storage=False)

    assert document == snapshot(
        {
            "key": "test",
            "value": {"string": '{"test": "test"}'},
            "created_at": datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
            "expires_at": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
        }
    )

    round_trip_managed_entry = document_to_managed_entry(document=document)

    assert round_trip_managed_entry.value == managed_entry.value
    assert round_trip_managed_entry.created_at == created_at
    assert round_trip_managed_entry.ttl == IsFloat(lt=0)
    assert round_trip_managed_entry.expires_at == expires_at


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestMongoDBStore(ContextManagerStoreTestMixin, BaseStoreTests):
    """Test MongoDBStore with native_storage=False (legacy mode) for backward compatibility."""

    @pytest.fixture(autouse=True, scope="session", params=MONGODB_VERSIONS_TO_TEST)
    async def setup_mongodb(self, request: pytest.FixtureRequest) -> AsyncGenerator[None, None]:
        version = request.param

        with docker_container(f"mongodb-test-{version}", f"mongo:{version}", {str(MONGODB_HOST_PORT): MONGODB_HOST_PORT}):
            if not await async_wait_for_true(bool_fn=ping_mongodb, tries=WAIT_FOR_MONGODB_TIMEOUT, wait_time=1):
                msg = f"MongoDB {version} failed to start"
                raise MongoDBFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_mongodb: None) -> MongoDBStore:
        # Use legacy mode (native_storage=False) to test backward compatibility
        store = MongoDBStore(url=f"mongodb://{MONGODB_HOST}:{MONGODB_HOST_PORT}", db_name=MONGODB_TEST_DB, native_storage=False)
        # Ensure a clean db by dropping our default test collection if it exists
        with contextlib.suppress(Exception):
            _ = await store._client.drop_database(name_or_database=MONGODB_TEST_DB)  # pyright: ignore[reportPrivateUsage]

        return store

    @pytest.fixture
    async def mongodb_store(self, store: MongoDBStore) -> MongoDBStore:
        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    async def test_mongodb_collection_name_sanitization(self, mongodb_store: MongoDBStore):
        """Tests that a special characters in the collection name will not raise an error."""
        await mongodb_store.put(collection="test_collection!@#$%^&*()", key="test_key", value={"test": "test"})
        assert await mongodb_store.get(collection="test_collection!@#$%^&*()", key="test_key") == {"test": "test"}

        collections = await mongodb_store.collections()
        assert collections == snapshot(["test_collection_-daf4a2ec"])


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestMongoDBStoreNativeMode(ContextManagerStoreTestMixin, BaseStoreTests):
    """Test MongoDBStore with native_storage=True (default)."""

    @pytest.fixture(autouse=True, scope="session", params=MONGODB_VERSIONS_TO_TEST)
    async def setup_mongodb(self, request: pytest.FixtureRequest) -> AsyncGenerator[None, None]:
        version = request.param

        with docker_container(f"mongodb-test-native-{version}", f"mongo:{version}", {str(MONGODB_HOST_PORT): MONGODB_HOST_PORT}):
            if not await async_wait_for_true(bool_fn=ping_mongodb, tries=WAIT_FOR_MONGODB_TIMEOUT, wait_time=1):
                msg = f"MongoDB {version} failed to start"
                raise MongoDBFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_mongodb: None) -> MongoDBStore:
        store = MongoDBStore(url=f"mongodb://{MONGODB_HOST}:{MONGODB_HOST_PORT}", db_name=f"{MONGODB_TEST_DB}-native", native_storage=True)
        # Ensure a clean db by dropping our default test collection if it exists
        with contextlib.suppress(Exception):
            _ = await store._client.drop_database(name_or_database=f"{MONGODB_TEST_DB}-native")  # pyright: ignore[reportPrivateUsage]

        return store

    @pytest.fixture
    async def mongodb_store(self, store: MongoDBStore) -> MongoDBStore:
        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    async def test_value_stored_as_bson_dict(self, mongodb_store: MongoDBStore):
        """Verify values are stored as BSON dicts, not JSON strings."""
        await mongodb_store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        # Get the raw MongoDB document
        await mongodb_store._setup_collection(collection="test")  # pyright: ignore[reportPrivateUsage]
        sanitized_collection = mongodb_store._sanitize_collection_name(collection="test")  # pyright: ignore[reportPrivateUsage]
        collection = mongodb_store._collections_by_name[sanitized_collection]  # pyright: ignore[reportPrivateUsage]
        doc = await collection.find_one({"key": "test_key"})

        # In native mode, value should be a dict with "dict" subfield
        assert doc is not None
        assert isinstance(doc["value"], dict)
        assert "dict" in doc["value"]
        assert doc["value"]["dict"] == {"name": "Alice", "age": 30}

    async def test_migration_from_legacy_mode(self, mongodb_store: MongoDBStore):
        """Verify native mode can read legacy JSON string data."""
        # Manually insert a legacy document with JSON string value in the new format
        await mongodb_store._setup_collection(collection="test")  # pyright: ignore[reportPrivateUsage]
        sanitized_collection = mongodb_store._sanitize_collection_name(collection="test")  # pyright: ignore[reportPrivateUsage]
        collection = mongodb_store._collections_by_name[sanitized_collection]  # pyright: ignore[reportPrivateUsage]

        await collection.insert_one(
            {
                "key": "legacy_key",
                "value": {"string": '{"legacy": "data"}'},  # New format with JSON string
            }
        )

        # Should be able to read it in native mode
        result = await mongodb_store.get(collection="test", key="legacy_key")
        assert result == {"legacy": "data"}

    async def test_migration_from_old_format(self, mongodb_store: MongoDBStore):
        """Verify native mode can read old format where value is directly a string."""
        # Manually insert an old document with value directly as JSON string
        await mongodb_store._setup_collection(collection="test")  # pyright: ignore[reportPrivateUsage]
        sanitized_collection = mongodb_store._sanitize_collection_name(collection="test")  # pyright: ignore[reportPrivateUsage]
        collection = mongodb_store._collections_by_name[sanitized_collection]  # pyright: ignore[reportPrivateUsage]

        await collection.insert_one(
            {
                "key": "old_key",
                "value": '{"old": "format"}',  # Old format: value directly as JSON string
            }
        )

        # Should be able to read it in native mode
        result = await mongodb_store.get(collection="test", key="old_key")
        assert result == {"old": "format"}
