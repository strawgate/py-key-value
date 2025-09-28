from collections.abc import AsyncGenerator

import pytest
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from tests.conftest import docker_container, docker_stop
from tests.stores.conftest import BaseStoreTests, ContextManagerStoreTestMixin, detect_on_windows, should_skip_docker_tests, wait_for_store

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
    def get_valkey_client(self):
        from glide_shared.config import GlideClientConfiguration, NodeAddress
        from glide_sync.glide_client import GlideClient

        client_config: GlideClientConfiguration = GlideClientConfiguration(
            addresses=[NodeAddress(host=VALKEY_HOST, port=VALKEY_PORT)], database_id=VALKEY_DB
        )
        return GlideClient.create(config=client_config)

    def ping_valkey(self) -> bool:
        try:
            client = self.get_valkey_client()
            _ = client.ping()
        except Exception:
            return False

        return True

    @pytest.fixture(scope="session")
    async def setup_valkey(self) -> AsyncGenerator[None, None]:
        # Double-check that the Redis test container is stopped
        docker_stop("redis-test", raise_on_error=False)

        with docker_container("valkey-test", "valkey/valkey:latest", {"6379": 6379}):
            if not wait_for_store(wait_fn=self.ping_valkey):
                msg = "Valkey failed to start"
                raise ValkeyFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_valkey: None):
        from key_value.aio.stores.valkey import ValkeyStore

        store: ValkeyStore = ValkeyStore(host=VALKEY_HOST, port=VALKEY_PORT, db=VALKEY_DB)

        # This is a syncronous client
        client = self.get_valkey_client()
        _ = client.flushdb()

        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
