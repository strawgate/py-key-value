import asyncio
from collections.abc import AsyncGenerator

import pytest
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from tests.conftest import docker_container, docker_stop, try_import
from tests.stores.conftest import BaseStoreTests, ContextManagerStoreTestMixin, detect_on_windows, should_skip_docker_tests

with try_import() as has_valkey:
    from glide.glide_client import GlideClient
    from glide_shared.config import GlideClientConfiguration, NodeAddress

    from key_value.aio.stores.valkey import ValkeyStore

if not has_valkey():
    pytestmark = pytest.mark.skip(reason="GlideClient is not installed")

# Valkey test configuration
VALKEY_HOST = "localhost"
VALKEY_PORT = 6379  # avoid clashing with Redis tests
VALKEY_DB = 15

WAIT_FOR_VALKEY_TIMEOUT = 30


class ValkeyFailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not running")
@pytest.mark.skipif(detect_on_windows(), reason="Valkey is not supported on Windows")
class TestValkeyStore(ContextManagerStoreTestMixin, BaseStoreTests):
    async def get_valkey_client(self):
        client_config: GlideClientConfiguration = GlideClientConfiguration(
            addresses=[NodeAddress(host=VALKEY_HOST, port=VALKEY_PORT)], database_id=VALKEY_DB
        )
        return await GlideClient.create(config=client_config)

    async def ping_valkey(self) -> bool:
        try:
            client = await self.get_valkey_client()
            _ = await client.ping()
        except Exception:
            return False

        return True

    async def wait_valkey(self) -> bool:
        for _ in range(WAIT_FOR_VALKEY_TIMEOUT):
            result = await asyncio.wait_for(self.ping_valkey(), timeout=1)
            if result:
                return True
            await asyncio.sleep(delay=1)
        return False

    @pytest.fixture(scope="session")
    async def setup_valkey(self) -> AsyncGenerator[None, None]:
        # Double-check that the Redis test container is stopped
        docker_stop("redis-test", raise_on_error=False)

        with docker_container("valkey-test", "valkey/valkey:latest", {"6379": 6379}):
            if not await self.wait_valkey():
                msg = "Valkey failed to start"
                raise ValkeyFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_valkey: None):
        store: ValkeyStore = ValkeyStore(host=VALKEY_HOST, port=VALKEY_PORT, db=VALKEY_DB)

        client: GlideClient = await self.get_valkey_client()
        _ = await client.flushdb()

        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
