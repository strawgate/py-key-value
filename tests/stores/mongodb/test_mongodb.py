import contextlib
from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from bson import ObjectId
from dirty_equals import IsDatetime, IsFloat, IsInstance
from inline_snapshot import snapshot
from pymongo import AsyncMongoClient
from testcontainers.mongodb import MongoDbContainer
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.mongodb import MongoDBStore
from key_value.aio.stores.mongodb.store import (
    MongoDBSerializationAdapter,
    MongoDBV1CollectionSanitizationStrategy,
)
from key_value.shared.managed_entry import ManagedEntry
from key_value.shared.wait import async_wait_for_true
from tests.conftest import should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

# MongoDB test configuration
MONGODB_TEST_DB = "kv-store-adapter-tests"

WAIT_FOR_MONGODB_TIMEOUT = 60

MONGODB_VERSIONS_TO_TEST = [
    "5.0",  # Older supported version
    "8.0",  # Latest stable version
]


async def ping_mongodb(mongodb_url: str) -> bool:
    try:
        client: AsyncMongoClient[Any] = AsyncMongoClient[Any](mongodb_url)
        _ = await client.list_database_names()
        await client.close()
    except Exception:
        return False

    return True


class MongoDBFailedToStartError(Exception):
    pass


def test_managed_entry_document_conversion():
    """Test that documents are stored as BSON dicts."""
    created_at = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    expires_at = created_at + timedelta(seconds=10)

    managed_entry = ManagedEntry(value={"test": "test"}, created_at=created_at, expires_at=expires_at)

    adapter = MongoDBSerializationAdapter()
    document = adapter.dump_dict(entry=managed_entry)

    assert document == snapshot(
        {
            "version": 1,
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


async def clean_mongodb_database(store: MongoDBStore) -> None:
    with contextlib.suppress(Exception):
        _ = await store._client.drop_database(name_or_database=store._db.name)  # pyright: ignore[reportPrivateUsage]


@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class BaseMongoDBStoreTests(ContextManagerStoreTestMixin, BaseStoreTests):
    """Base class for MongoDB store tests."""

    @pytest.fixture(autouse=True, scope="module", params=MONGODB_VERSIONS_TO_TEST)
    def mongodb_container(self, request: pytest.FixtureRequest) -> Generator[MongoDbContainer, None, None]:
        version = request.param
        with MongoDbContainer(image=f"mongo:{version}") as container:
            yield container

    @pytest.fixture(scope="module")
    def mongodb_url(self, mongodb_container: MongoDbContainer) -> str:
        return mongodb_container.get_connection_url()

    @pytest.fixture(autouse=True, scope="module")
    async def setup_mongodb(self, mongodb_container: MongoDbContainer, mongodb_url: str) -> None:
        if not await async_wait_for_true(bool_fn=lambda: ping_mongodb(mongodb_url), tries=WAIT_FOR_MONGODB_TIMEOUT, wait_time=1):
            msg = "MongoDB failed to start"
            raise MongoDBFailedToStartError(msg)

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    @override
    async def test_long_collection_name(self, store: MongoDBStore, sanitizing_store: MongoDBStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        with pytest.raises(Exception):  # noqa: B017, PT011
            await store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})

        await sanitizing_store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection" * 100, key="test_key") == {"test": "test"}

    @override
    async def test_special_characters_in_collection_name(self, store: MongoDBStore, sanitizing_store: MongoDBStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Tests that special characters in the collection name will not raise an error."""
        with pytest.raises(Exception):  # noqa: B017, PT011
            await store.put(collection="test_collection!@#$%^&*()", key="test_key", value={"test": "test"})

        await sanitizing_store.put(collection="test_collection!@#$%^&*()", key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection!@#$%^&*()", key="test_key") == {"test": "test"}

    async def test_mongodb_collection_name_sanitization(self, sanitizing_store: MongoDBStore):
        """Tests that the V1 sanitization strategy produces the expected collection names."""
        await sanitizing_store.put(collection="test_collection!@#$%^&*()", key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection!@#$%^&*()", key="test_key") == {"test": "test"}

        collections = sanitizing_store._collections_by_name.values()
        collection_names = [collection.name for collection in collections]
        assert collection_names == snapshot(["S_test_collection_-daf4a2ec"])


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestMongoDBStore(BaseMongoDBStoreTests):
    """Test MongoDBStore with native BSON storage."""

    @override
    @pytest.fixture
    async def store(self, setup_mongodb: None, mongodb_url: str) -> MongoDBStore:
        store = MongoDBStore(url=mongodb_url, db_name=MONGODB_TEST_DB)

        await clean_mongodb_database(store=store)

        return store

    @pytest.fixture
    async def sanitizing_store(self, setup_mongodb: None, mongodb_url: str) -> MongoDBStore:
        store = MongoDBStore(
            url=mongodb_url,
            db_name=f"{MONGODB_TEST_DB}-sanitizing",
            collection_sanitization_strategy=MongoDBV1CollectionSanitizationStrategy(),
        )

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
                "collection": "test",
                "created_at": IsDatetime(),
                "value": {"object": {"name": "Alice", "age": 30}},
                "version": 1,
            }
        )
