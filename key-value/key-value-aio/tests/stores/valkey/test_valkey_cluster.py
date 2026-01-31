"""Tests for ValkeyStore with GlideClusterClient support.

This test file includes:
1. Type compatibility tests - verify ValkeyStore accepts GlideClusterClient types
2. Integration tests - run against a live 6-node Valkey cluster via docker-compose

The cluster tests use the Bitnami valkey-cluster image which sets up:
- 3 primary nodes
- 3 replica nodes (1 replica per primary)
- Automatic cluster initialization and slot distribution
"""

import contextlib
import json
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from dirty_equals import IsDatetime
from inline_snapshot import snapshot
from key_value.shared.stores.wait import async_wait_for_true
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from tests.conftest import detect_on_windows, docker_compose_cluster, should_skip_docker_compose_tests
from tests.stores.base import (
    BaseStoreTests,
    ContextManagerStoreTestMixin,
)

# Cluster test configuration
VALKEY_CLUSTER_HOST = "localhost"
VALKEY_CLUSTER_PORTS = [7000, 7001, 7002, 7003, 7004, 7005]
COMPOSE_FILE = Path(__file__).parent / "docker-compose-cluster.yml"

WAIT_FOR_CLUSTER_TIMEOUT = 60


class ValkeyClusterFailedToStartError(Exception):
    pass


class TestValkeyClusterClientSupport:
    """Tests for GlideClusterClient type compatibility with ValkeyStore."""

    async def test_cluster_client_type_accepted(self):
        """Verify that ValkeyStore's type hints accept GlideClusterClient.

        This test verifies that the type system recognizes GlideClusterClient
        as a valid client type for ValkeyStore. It does not test runtime
        functionality against a live cluster.
        """
        from glide.glide_client import GlideClusterClient

        from key_value.aio.stores.valkey import ValkeyStore

        # Verify the import works
        assert GlideClusterClient is not None

        # Type checker should accept this (verified at typecheck time)
        # This line demonstrates that ValkeyStore.__init__ accepts GlideClusterClient
        # We don't actually call it because we don't have a live cluster
        _: type[ValkeyStore] = ValkeyStore

        # Verify GlideClusterClient is imported in the store module
        from key_value.aio.stores.valkey.store import GlideClusterClient as ImportedClient

        assert ImportedClient is GlideClusterClient

    async def test_store_docstring_mentions_cluster(self):
        """Verify that ValkeyStore documentation mentions cluster support."""
        from key_value.aio.stores.valkey import ValkeyStore

        assert ValkeyStore.__doc__ is not None
        assert "cluster" in ValkeyStore.__doc__.lower() or "GlideClusterClient" in ValkeyStore.__doc__

    async def test_cluster_config_type_accepted(self):
        """Verify that GlideClusterClientConfiguration is imported and usable."""
        from glide_shared.config import GlideClusterClientConfiguration

        assert GlideClusterClientConfiguration is not None

        # Verify it's imported in the store module
        from key_value.aio.stores.valkey.store import GlideClusterClientConfiguration as ImportedConfig

        assert ImportedConfig is GlideClusterClientConfiguration


@pytest.mark.skipif(should_skip_docker_compose_tests(), reason="Docker Compose is not available")
@pytest.mark.skipif(detect_on_windows(), reason="Valkey is not supported on Windows")
class TestValkeyClusterIntegration(ContextManagerStoreTestMixin, BaseStoreTests):
    """Full integration tests for ValkeyStore with a real Valkey cluster.

    These tests run against a 6-node Valkey cluster set up via docker-compose.
    The cluster uses the Bitnami valkey-cluster image which automatically:
    - Creates 3 primary nodes
    - Creates 3 replica nodes
    - Initializes the cluster and distributes hash slots
    """

    async def get_valkey_cluster_client(self):
        """Get a GlideClusterClient connected to the cluster."""
        from glide.glide_client import GlideClusterClient
        from glide_shared.config import GlideClusterClientConfiguration, NodeAddress

        addresses = [NodeAddress(host=VALKEY_CLUSTER_HOST, port=port) for port in VALKEY_CLUSTER_PORTS]
        client_config = GlideClusterClientConfiguration(addresses=addresses)
        return await GlideClusterClient.create(config=client_config)

    async def ping_cluster(self) -> bool:
        """Check if the cluster is ready and responding."""
        client = None
        try:
            client = await self.get_valkey_cluster_client()
            # Ping the cluster
            await client.ping()
            # Also check cluster state
            info = await client.custom_command(["CLUSTER", "INFO"])  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if isinstance(info, bytes):
                info_str = info.decode("utf-8")
                if "cluster_state:ok" in info_str:
                    return True
        except Exception:
            return False
        finally:
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.close()
        return False

    @pytest.fixture(scope="class")
    async def setup_valkey_cluster(self) -> AsyncGenerator[None, None]:
        """Set up the Valkey cluster using docker-compose."""
        with docker_compose_cluster(
            compose_file=COMPOSE_FILE,
            project_name="valkey-cluster-test",
            timeout=120,
        ):
            if not await async_wait_for_true(bool_fn=self.ping_cluster, tries=WAIT_FOR_CLUSTER_TIMEOUT, wait_time=2):
                msg = "Valkey cluster failed to start"
                raise ValkeyClusterFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_valkey_cluster: None):
        """Get a ValkeyStore connected to the cluster."""
        from key_value.aio.stores.valkey import ValkeyStore

        client = await self.get_valkey_cluster_client()

        # Flush all keys from all nodes
        await client.custom_command(["FLUSHALL"])  # pyright: ignore[reportUnknownMemberType]

        # Create store with the cluster client
        return ValkeyStore(client=client)

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    async def test_cluster_value_stored(self, store: BaseStore):
        """Test that values are stored correctly in the cluster."""
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

    async def test_cluster_multi_key_operations(self, store: BaseStore):
        """Test multi-key operations work correctly in cluster mode.

        In cluster mode, MGET/MSET operations on keys that hash to different slots
        are automatically handled by the GLIDE client, which splits them into
        sub-commands sent to the appropriate nodes and aggregates the results.
        """
        # Put multiple keys that will likely hash to different slots
        keys = ["user:1", "user:2", "user:3", "user:4", "user:5"]
        for key in keys:
            await store.put(collection="test", key=key, value={"key": key})

        # Get all keys at once - this tests cross-slot MGET handling
        results = await store.get_many(collection="test", keys=keys)

        assert len(results) == len(keys)
        for key, value in zip(keys, results, strict=True):
            assert value is not None
            assert value["key"] == key

    async def test_cluster_delete_many(self, store: BaseStore):
        """Test bulk delete operations in cluster mode."""
        # Put multiple keys
        keys = ["delete:1", "delete:2", "delete:3"]
        for key in keys:
            await store.put(collection="test", key=key, value={"key": key})

        # Verify they exist
        for key in keys:
            value = await store.get(collection="test", key=key)
            assert value is not None

        # Delete all at once
        count = await store.delete_many(collection="test", keys=keys)
        assert count == len(keys)

        # Verify they're gone
        for key in keys:
            value = await store.get(collection="test", key=key)
            assert value is None

    async def test_cluster_ttl(self, store: BaseStore):
        """Test TTL operations work in cluster mode."""
        await store.put(collection="test", key="ttl_key", value={"data": "test"}, ttl=60)

        # Get TTL - returns (value, ttl) tuple
        value, ttl = await store.ttl(collection="test", key="ttl_key")
        assert value is not None
        assert ttl is not None
        assert 55 < ttl <= 60  # Allow some time drift

    async def test_cluster_key_distribution(self, store: BaseStore):
        """Test that keys are distributed across the cluster.

        This test verifies that different keys are routed to different
        slots/nodes, demonstrating the cluster is working correctly.
        """
        from key_value.aio.stores.valkey import ValkeyStore

        assert isinstance(store, ValkeyStore)
        client = store._connected_client  # pyright: ignore[reportPrivateUsage]
        assert client is not None

        # Put keys that will hash to different slots
        keys = ["key:a", "key:b", "key:c", "key:d", "key:e"]
        for key in keys:
            await store.put(collection="test", key=key, value={"key": key})

        # Get the slot for each key using CLUSTER KEYSLOT
        slots: set[int] = set()
        for key in keys:
            full_key = f"test::{key}"
            slot_response = await client.custom_command(["CLUSTER", "KEYSLOT", full_key])  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportAttributeAccessIssue]
            if isinstance(slot_response, int):
                slots.add(slot_response)

        # With 5 different keys, we should have at least 2 different slots
        # (unless we're very unlucky with the hash distribution)
        assert len(slots) >= 2, f"Expected keys to hash to multiple slots, got: {slots}"
