import contextlib
import json

import pytest
from dirty_equals import IsDatetime
from inline_snapshot import snapshot
from testcontainers.core.container import DockerContainer
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.shared.wait import async_wait_for_true
from tests.conftest import detect_on_windows, should_skip_docker_tests
from tests.stores.base import (
    BaseStoreTests,
    ContextManagerStoreTestMixin,
)

# Valkey test configuration
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
    async def get_valkey_client(self, host: str, port: int):
        from key_value.aio.stores.valkey.store import _create_valkey_client, _create_valkey_client_config

        config = _create_valkey_client_config(host=host, port=port, db=VALKEY_DB)
        return await _create_valkey_client(config)

    async def ping_valkey(self, host: str, port: int) -> bool:
        client = None
        try:
            client = await self.get_valkey_client(host, port)
            await client.ping()
        except Exception:
            return False
        else:
            return True
        finally:
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.close()

    @pytest.fixture(autouse=True, scope="module", params=VALKEY_VERSIONS_TO_TEST)
    def valkey_container(self, request: pytest.FixtureRequest):
        version = request.param
        container = DockerContainer(image=f"valkey/valkey:{version}")
        container.with_exposed_ports(VALKEY_CONTAINER_PORT)
        with container:
            yield container

    @pytest.fixture(scope="module")
    def valkey_host(self, valkey_container: DockerContainer) -> str:
        return valkey_container.get_container_host_ip()

    @pytest.fixture(scope="module")
    def valkey_port(self, valkey_container: DockerContainer) -> int:
        return int(valkey_container.get_exposed_port(VALKEY_CONTAINER_PORT))

    @pytest.fixture(autouse=True, scope="module")
    async def setup_valkey(self, valkey_container: DockerContainer, valkey_host: str, valkey_port: int) -> None:
        ready = await async_wait_for_true(
            bool_fn=lambda: self.ping_valkey(valkey_host, valkey_port),
            tries=WAIT_FOR_VALKEY_TIMEOUT,
            wait_time=1,
        )
        if not ready:
            msg = "Valkey failed to start"
            raise ValkeyFailedToStartError(msg)

    @override
    @pytest.fixture
    async def store(self, setup_valkey: None, valkey_host: str, valkey_port: int):
        from key_value.aio.stores.valkey import ValkeyStore

        store: ValkeyStore = ValkeyStore(host=valkey_host, port=valkey_port, db=VALKEY_DB)

        # This is a syncronous client
        client = await self.get_valkey_client(valkey_host, valkey_port)
        try:
            _ = await client.flushdb()
        finally:
            with contextlib.suppress(Exception):
                await client.close()

        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    async def test_value_stored(self, store: BaseStore):
        from key_value.aio.stores.valkey import ValkeyStore

        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        assert isinstance(store, ValkeyStore)

        valkey_client = store._connected_client
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
