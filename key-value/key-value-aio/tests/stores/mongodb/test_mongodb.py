import contextlib
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from bson import ObjectId
from dirty_equals import IsDatetime, IsFloat, IsInstance
from inline_snapshot import snapshot
from key_value.shared.stores.wait import async_wait_for_true
from key_value.shared.utils.managed_entry import ManagedEntry
from pymongo import AsyncMongoClient
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.mongodb import MongoDBStore
from key_value.aio.stores.mongodb.store import MongoDBSerializationAdapter
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

    adapter = MongoDBSerializationAdapter(native_storage=True)
    document = adapter.dump_dict(entry=managed_entry)

    assert document == snapshot(
        {
            "value": {"object": {"test": "test"}},
            "created_at": datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
            "expires_at": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
        }
    )

    round_trip_managed_entry = adapter.load_dict(data=document)

    assert round_trip_managed_entry.value == managed_entry.value
    assert round_trip_managed_entry.created_at == created_at
    assert round_trip_managed_entry.ttl == IsFloat(lt=0)
    assert round_trip_managed_entry.expires_at == expires_at


def test_managed_entry_document_conversion_legacy_mode():
    created_at = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    expires_at = created_at + timedelta(seconds=10)

    managed_entry = ManagedEntry(value={"test": "test"}, created_at=created_at, expires_at=expires_at)
    adapter = MongoDBSerializationAdapter(native_storage=False)
    document = adapter.dump_dict(entry=managed_entry)

    assert document == snapshot(
        {
            "value": {"string": '{"test": "test"}'},
            "created_at": datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
            "expires_at": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
        }
    )

    round_trip_managed_entry = adapter.load_dict(data=document)

    assert round_trip_managed_entry.value == managed_entry.value
    assert round_trip_managed_entry.created_at == created_at
    assert round_trip_managed_entry.ttl == IsFloat(lt=0)
    assert round_trip_managed_entry.expires_at == expires_at


async def clean_mongodb_database(store: MongoDBStore) -> None:
    with contextlib.suppress(Exception):
        _ = await store._client.drop_database(name_or_database=store._db.name)  # pyright: ignore[reportPrivateUsage]


@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class BaseMongoDBStoreTests(ContextManagerStoreTestMixin, BaseStoreTests):
    """Base class for MongoDB store tests."""

    @pytest.fixture(autouse=True, scope="session", params=MONGODB_VERSIONS_TO_TEST)
    async def setup_mongodb(self, request: pytest.FixtureRequest) -> AsyncGenerator[None, None]:
        version = request.param

        with docker_container(f"mongodb-test-{version}", f"mongo:{version}", {str(MONGODB_HOST_PORT): MONGODB_HOST_PORT}):
            if not await async_wait_for_true(bool_fn=ping_mongodb, tries=WAIT_FOR_MONGODB_TIMEOUT, wait_time=1):
                msg = f"MongoDB {version} failed to start"
                raise MongoDBFailedToStartError(msg)

            yield

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    async def test_mongodb_collection_name_sanitization(self, store: MongoDBStore):
        """Tests that a special characters in the collection name will not raise an error."""
        await store.put(collection="test_collection!@#$%^&*()", key="test_key", value={"test": "test"})
        assert await store.get(collection="test_collection!@#$%^&*()", key="test_key") == {"test": "test"}

        collections = store._collections_by_name.values()
        collection_names = [collection.name for collection in collections]
        assert collection_names == snapshot(["S_test_collection_-daf4a2ec"])


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestMongoDBStoreNativeMode(BaseMongoDBStoreTests):
    """Test MongoDBStore with native_storage=True (default)."""

    @override
    @pytest.fixture
    async def store(self, setup_mongodb: None) -> MongoDBStore:
        store = MongoDBStore(url=f"mongodb://{MONGODB_HOST}:{MONGODB_HOST_PORT}", db_name=f"{MONGODB_TEST_DB}-native", native_storage=True)

        await clean_mongodb_database(store=store)

        return store

    async def test_value_stored_as_bson_dict(self, store: MongoDBStore):
        """Verify values are stored as BSON dicts, not JSON strings."""
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        # Get the raw MongoDB document
        await store._setup_collection(collection="test")  # pyright: ignore[reportPrivateUsage]
        sanitized_collection = store._sanitize_collection(collection="test")  # pyright: ignore[reportPrivateUsage]
        collection = store._collections_by_name[sanitized_collection]  # pyright: ignore[reportPrivateUsage]
        doc = await collection.find_one({"key": "test_key"})

        assert doc == snapshot(
            {
                "_id": IsInstance(expected_type=ObjectId),
                "key": "test_key",
                "created_at": IsDatetime(),
                "value": {"object": {"name": "Alice", "age": 30}},
            }
        )

    async def test_migration_from_legacy_mode(self, store: MongoDBStore):
        """Verify native mode can read legacy JSON string data."""
        await store._setup_collection(collection="test")  # pyright: ignore[reportPrivateUsage]
        sanitized_collection = store._sanitize_collection(collection="test")  # pyright: ignore[reportPrivateUsage]
        collection = store._collections_by_name[sanitized_collection]  # pyright: ignore[reportPrivateUsage]

        await collection.insert_one(
            {
                "key": "legacy_key",
                "value": {"string": '{"legacy": "data"}'},
            }
        )

        result = await store.get(collection="test", key="legacy_key")
        assert result == {"legacy": "data"}


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestMongoDBStoreNonNativeMode(BaseMongoDBStoreTests):
    """Test MongoDBStore with native_storage=False (legacy mode) for backward compatibility."""

    @override
    @pytest.fixture
    async def store(self, setup_mongodb: None) -> MongoDBStore:
        store = MongoDBStore(url=f"mongodb://{MONGODB_HOST}:{MONGODB_HOST_PORT}", db_name=MONGODB_TEST_DB, native_storage=False)

        await clean_mongodb_database(store=store)

        return store

    async def test_value_stored_as_json(self, store: MongoDBStore):
        """Verify values are stored as JSON strings."""
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        # Get the raw MongoDB document
        await store._setup_collection(collection="test")  # pyright: ignore[reportPrivateUsage]
        sanitized_collection = store._sanitize_collection(collection="test")  # pyright: ignore[reportPrivateUsage]
        collection = store._collections_by_name[sanitized_collection]  # pyright: ignore[reportPrivateUsage]
        doc = await collection.find_one({"key": "test_key"})

        assert doc == snapshot(
            {
                "_id": IsInstance(expected_type=ObjectId),
                "key": "test_key",
                "created_at": IsDatetime(),
                "value": {"string": '{"age": 30, "name": "Alice"}'},
            }
        )

    async def test_migration_from_native_mode(self, store: MongoDBStore):
        """Verify non-native mode can read native mode data."""
        await store._setup_collection(collection="test")  # pyright: ignore[reportPrivateUsage]
        sanitized_collection = store._sanitize_collection(collection="test")  # pyright: ignore[reportPrivateUsage]
        collection = store._collections_by_name[sanitized_collection]  # pyright: ignore[reportPrivateUsage]

        await collection.insert_one(
            {
                "key": "legacy_key",
                "value": {"object": {"name": "Alice", "age": 30}},
            }
        )

        result = await store.get(collection="test", key="legacy_key")
        assert result == {"name": "Alice", "age": 30}
