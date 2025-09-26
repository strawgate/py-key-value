import asyncio
from collections.abc import AsyncGenerator

import pytest
from glide.glide_client import GlideClient
from glide_shared.config import GlideClientConfiguration, NodeAddress
from typing_extensions import override

from kv_store_adapter.stores.base import BaseStore
from kv_store_adapter.stores.valkey import ValkeyStore
from tests.stores.conftest import BaseStoreTests, ContextManagerStoreTestMixin, should_skip_docker_tests

# Valkey test configuration
VALKEY_HOST = "localhost"
VALKEY_PORT = 6379  # avoid clashing with Redis tests
VALKEY_DB = 15

WAIT_FOR_VALKEY_TIMEOUT = 30


async def get_valkey_client() -> GlideClient:
    client_config: GlideClientConfiguration = GlideClientConfiguration(
        addresses=[NodeAddress(host=VALKEY_HOST, port=VALKEY_PORT)], database_id=VALKEY_DB
    )
    return await GlideClient.create(config=client_config)


async def ping_valkey() -> bool:
    try:
        client = await get_valkey_client()
        _ = await client.ping()
    except Exception:
        return False

    return True


async def wait_valkey() -> bool:
    for _ in range(WAIT_FOR_VALKEY_TIMEOUT):
        if await ping_valkey():
            return True
        await asyncio.sleep(delay=1)
    return False


class ValkeyFailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not running")
@pytest.mark.timeout(15)
class TestValkeyStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session")
    async def setup_valkey(self) -> AsyncGenerator[None, None]:
        _ = await asyncio.create_subprocess_exec("docker", "stop", "valkey-test")
        _ = await asyncio.create_subprocess_exec("docker", "rm", "-f", "valkey-test")

        process = await asyncio.create_subprocess_exec(
            "docker", "run", "-d", "--name", "valkey-test", "-p", f"{VALKEY_PORT}:6379", "valkey/valkey:latest"
        )
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
    async def store(self, setup_valkey: None) -> ValkeyStore:
        store: ValkeyStore = ValkeyStore(host=VALKEY_HOST, port=VALKEY_PORT, db=VALKEY_DB)

        client: GlideClient = await get_valkey_client()
        _ = await client.flushdb()

        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
