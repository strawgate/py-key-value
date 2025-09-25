import asyncio
from collections.abc import AsyncGenerator

import pytest
from typing_extensions import override
from valkey.asyncio import Valkey

from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.stores.valkey import ValkeyStore
from tests.stores.conftest import BaseStoreTests

# Valkey test configuration
VALKEY_HOST = "localhost"
VALKEY_PORT = 6379
VALKEY_DB = 15  # Use a separate database for tests

WAIT_FOR_VALKEY_TIMEOUT = 30


async def ping_valkey() -> bool:
    client = Valkey(host=VALKEY_HOST, port=VALKEY_PORT, db=VALKEY_DB, decode_responses=True)
    try:
        return await client.ping()  # pyright: ignore[reportUnknownMemberType, reportAny]
    except Exception:
        return False


async def wait_valkey() -> bool:
    # with a timeout of 30 seconds
    for _ in range(WAIT_FOR_VALKEY_TIMEOUT):
        if await ping_valkey():
            return True
        await asyncio.sleep(delay=1)

    return False


class ValkeyFailedToStartError(Exception):
    pass


class TestValkeyStore(BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session")
    async def setup_valkey(self) -> AsyncGenerator[None, None]:
        _ = await asyncio.create_subprocess_exec("docker", "stop", "valkey-test")
        _ = await asyncio.create_subprocess_exec("docker", "rm", "-f", "valkey-test")

        process = await asyncio.create_subprocess_exec("docker", "run", "-d", "--name", "valkey-test", "-p", "6379:6379", "valkey/valkey")
        _ = await process.wait()
        if not await wait_valkey():
            msg = "Valkey failed to start"
            raise ValkeyFailedToStartError(msg)
        try:
            yield
        finally:
            _ = await asyncio.create_subprocess_exec("docker", "rm", "-f", "valkey-test")

    @override
    @pytest.fixture
    async def store(self, setup_valkey: ValkeyStore) -> ValkeyStore:
        """Create a Valkey store for testing."""
        # Create the store with test database
        valkey_store = ValkeyStore(host=VALKEY_HOST, port=VALKEY_PORT, db=VALKEY_DB)
        _ = await valkey_store._client.flushdb()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType]
        return valkey_store

    async def test_valkey_url_connection(self):
        """Test Valkey store creation with URL."""
        valkey_url = f"valkey://{VALKEY_HOST}:{VALKEY_PORT}/{VALKEY_DB}"
        store = ValkeyStore(url=valkey_url)
        _ = await store._client.flushdb()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType]
        await store.put(collection="test", key="url_test", value={"test": "value"})
        result = await store.get(collection="test", key="url_test")
        assert result == {"test": "value"}

    async def test_valkey_client_connection(self):
        """Test Valkey store creation with existing client."""
        client = Valkey(host=VALKEY_HOST, port=VALKEY_PORT, db=VALKEY_DB, decode_responses=True)
        store = ValkeyStore(client=client)

        _ = await store._client.flushdb()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType]
        await store.put(collection="test", key="client_test", value={"test": "value"})
        result = await store.get(collection="test", key="client_test")
        assert result == {"test": "value"}

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseKVStore): ...