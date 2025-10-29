import contextlib
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone

import pytest
from inline_snapshot import snapshot
from key_value.shared.stores.wait import async_wait_for_true
from key_value.shared.utils.compound import compound_key
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

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


def test_managed_entry_serialization():
    """Test ManagedEntry serialization to JSON for Valkey storage."""
    created_at = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    expires_at = created_at + timedelta(seconds=10)

    managed_entry = ManagedEntry(value={"test": "test"}, created_at=created_at, expires_at=expires_at)
    json_str = managed_entry.to_json()

    assert json_str == snapshot('{"value": {"test": "test"}}')

    round_trip_managed_entry = ManagedEntry.from_json(json_str=json_str)

    assert round_trip_managed_entry.value == managed_entry.value


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

    async def test_value_stored_as_json_string(self, store):
        """Verify values are stored as JSON strings in Valkey."""
        from typing import TYPE_CHECKING

        if TYPE_CHECKING:
            from key_value.aio.stores.valkey import ValkeyStore

        store: ValkeyStore  # type: ignore[name-defined]
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        # Get raw Valkey value using the compound key format
        combo_key = compound_key(collection="test", key="test_key")
        raw_value = await store._client.get(key=combo_key)  # pyright: ignore[reportPrivateUsage]

        # Decode bytes to string
        assert isinstance(raw_value, bytes)
        decoded_value = raw_value.decode("utf-8")

        assert decoded_value == snapshot('{"value": {"name": "Alice", "age": 30}}')

        # Test with TTL to verify it still stores correctly
        await store.put(collection="test", key="test_key_ttl", value={"name": "Bob", "age": 25}, ttl=3600)
        combo_key_ttl = compound_key(collection="test", key="test_key_ttl")
        raw_value_ttl = await store._client.get(key=combo_key_ttl)  # pyright: ignore[reportPrivateUsage]

        assert isinstance(raw_value_ttl, bytes)
        decoded_value_ttl = raw_value_ttl.decode("utf-8")

        assert decoded_value_ttl == snapshot('{"value": {"name": "Bob", "age": 25}}')
