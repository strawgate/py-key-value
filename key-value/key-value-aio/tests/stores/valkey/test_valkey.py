import contextlib
import json
from collections.abc import AsyncGenerator

import pytest
from dirty_equals import IsDatetime
from inline_snapshot import snapshot
from typing_extensions import override

from key_value.aio._shared.stores.wait import async_wait_for_true
from key_value.aio.stores.base import BaseStore
from tests.conftest import detect_on_windows, docker_container, should_skip_docker_tests
from tests.stores.base import (
    BaseStoreTests,
    ContextManagerStoreTestMixin,
)

# Valkey test configuration
VALKEY_HOST = "localhost"
VALKEY_PORT = 6380  # normally 6379, avoid clashing with Redis tests
VALKEY_DB = 15
VALKEY_CONTAINER_PORT = 6379

WAIT_FOR_VALKEY_TIMEOUT = 30

VALKEY_VERSIONS_TO_TEST = [
    "7.2.5",  # Released Apr 2024
    "8.0.0",  # Released Sep 2024
    "9.0.0",  # Released Oct 2025
]


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
        client = None
        try:
            client = await self.get_valkey_client()
            await client.ping()
        except Exception:
            return False
        else:
            return True
        finally:
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.close()

    @pytest.fixture(scope="session", params=VALKEY_VERSIONS_TO_TEST)
    async def setup_valkey(self, request: pytest.FixtureRequest) -> AsyncGenerator[None, None]:
        version = request.param

        with docker_container(f"valkey-test-{version}", f"valkey/valkey:{version}", {str(VALKEY_CONTAINER_PORT): VALKEY_PORT}):
            if not await async_wait_for_true(bool_fn=self.ping_valkey, tries=WAIT_FOR_VALKEY_TIMEOUT, wait_time=1):
                msg = f"Valkey {version} failed to start"
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

    async def test_value_stored(self, store: BaseStore):
        from key_value.aio.stores.valkey import ValkeyStore

        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        assert isinstance(store, ValkeyStore)

        valkey_client = store._connected_client  # pyright: ignore[reportPrivateUsage]
        assert valkey_client is not None
        value = await valkey_client.get(key="test::test_key")
        assert value is not None
        value_as_dict = json.loads(value.decode("utf-8"))
        assert value_as_dict == snapshot(
            {
                "collection": "test",
                "created_at": IsDatetime(iso_string=True),
                "key": "test_key",
                "value": {"age": 30, "name": "Alice"},
                "version": 1,
            }
        )

        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30}, ttl=10)

        value = await valkey_client.get(key="test::test_key")
        assert value is not None
        value_as_dict = json.loads(value.decode("utf-8"))
        assert value_as_dict == snapshot(
            {
                "collection": "test",
                "created_at": IsDatetime(iso_string=True),
                "value": {"age": 30, "name": "Alice"},
                "key": "test_key",
                "expires_at": IsDatetime(iso_string=True),
                "version": 1,
            }
        )
