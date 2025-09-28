import contextlib
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from inline_snapshot import snapshot
from pymongo import MongoClient
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.mongodb import MongoDBStore
from tests.conftest import docker_container
from tests.stores.conftest import BaseStoreTests, ContextManagerStoreTestMixin, should_skip_docker_tests, wait_for_store

# MongoDB test configuration
MONGODB_HOST = "localhost"
MONGODB_HOST_PORT = 27017
MONGODB_TEST_DB = "kv-store-adapter-tests"

WAIT_FOR_MONGODB_TIMEOUT = 30


def ping_mongodb() -> bool:
    try:
        client: MongoClient[Any] = MongoClient[Any](host=MONGODB_HOST, port=MONGODB_HOST_PORT)
        _ = client.list_database_names()
    except Exception:
        return False

    return True


class MongoDBFailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestMongoDBStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session")
    async def setup_mongodb(self) -> AsyncGenerator[None, None]:
        with docker_container("mongodb-test", "mongo:7", {"27017": 27017}):
            if not wait_for_store(wait_fn=ping_mongodb):
                msg = "MongoDB failed to start"
                raise MongoDBFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_mongodb: None) -> MongoDBStore:
        store = MongoDBStore(url=f"mongodb://{MONGODB_HOST}:{MONGODB_HOST_PORT}", db_name=MONGODB_TEST_DB)
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
