import asyncio
import contextlib
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from inline_snapshot import snapshot
from pymongo import AsyncMongoClient
from typing_extensions import override

from kv_store_adapter.stores.base import BaseStore
from kv_store_adapter.stores.mongodb import MongoDBStore
from tests.stores.conftest import BaseStoreTests, ContextManagerStoreTestMixin, should_skip_docker_tests

# MongoDB test configuration
MONGODB_HOST = "localhost"
MONGODB_HOST_PORT = 27017
MONGODB_TEST_DB = "kv-store-adapter-tests"

WAIT_FOR_MONGODB_TIMEOUT = 30


async def ping_mongodb() -> bool:
    try:
        client: AsyncMongoClient[Any] = AsyncMongoClient[Any](host=MONGODB_HOST, port=MONGODB_HOST_PORT)
        _ = await client.list_database_names()
    except Exception:
        return False

    return True


async def wait_mongodb() -> bool:
    for _ in range(WAIT_FOR_MONGODB_TIMEOUT):
        if await ping_mongodb():
            return True
        await asyncio.sleep(delay=1)
    return False


class MongoDBFailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestMongoDBStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session")
    async def setup_mongodb(self) -> AsyncGenerator[None, None]:
        _ = await asyncio.create_subprocess_exec("docker", "stop", "mongodb-test")
        _ = await asyncio.create_subprocess_exec("docker", "rm", "-f", "mongodb-test")

        process = await asyncio.create_subprocess_exec(
            "docker", "run", "-d", "--name", "mongodb-test", "-p", f"{MONGODB_HOST_PORT}:27017", "mongo:7"
        )
        _ = await process.wait()
        if not await wait_mongodb():
            msg = "MongoDB failed to start"
            raise MongoDBFailedToStartError(msg)
        try:
            yield
        finally:
            _ = await asyncio.create_subprocess_exec("docker", "rm", "-f", "mongodb-test")

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
