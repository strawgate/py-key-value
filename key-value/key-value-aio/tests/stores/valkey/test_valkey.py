from collections.abc import AsyncGenerator

import pytest
from key_value.shared.stores.wait import async_wait_for_true
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from tests.conftest import detect_on_windows, docker_container, docker_stop, should_skip_docker_tests
from tests.stores.base import (
    BaseStoreTests,
    ContextManagerStoreTestMixin,
)

# Valkey test configuration
VALKEY_HOST = "localhost"
VALKEY_PORT = 6380  # normally 6379, avoid clashing with Redis tests
VALKEY_DB = 15

WAIT_FOR_VALKEY_TIMEOUT = 30


class ValkeyFailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not running")
@pytest.mark.skipif(detect_on_windows(), reason="Valkey is not supported on Windows")
class TestValkeyStore(ContextManagerStoreTestMixin, BaseStoreTests):
    async def get_valkey_client(self):
        from glide.glide_client import GlideClient
        from glide_shared.config import GlideClientConfiguration, NodeAddress

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

    @pytest.fixture(scope="session")
    async def setup_valkey(self) -> AsyncGenerator[None, None]:
        # Double-check that the Redis test container is stopped
        docker_stop("redis-test", raise_on_error=False)

        with docker_container("valkey-test", "valkey/valkey:latest", {"6379": 6380}):
            if not await async_wait_for_true(bool_fn=self.ping_valkey, tries=30, wait_time=1):
                msg = "Valkey failed to start"
                raise ValkeyFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_valkey: None):
        from key_value.aio.stores.valkey import ValkeyStore

        store: ValkeyStore = ValkeyStore(host=VALKEY_HOST, port=VALKEY_PORT, db=VALKEY_DB)

        # This is a syncronous client
        client = await self.get_valkey_client()
        _ = await client.flushdb()

        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
