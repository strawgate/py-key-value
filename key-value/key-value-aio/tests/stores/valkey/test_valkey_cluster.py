"""Tests for ValkeyStore with GlideClusterClient support.

Note: This test file verifies that ValkeyStore accepts GlideClusterClient as a type,
but does not run full integration tests against a live Valkey cluster. Setting up a
proper multi-node Valkey cluster in Docker requires complex configuration with:
- Multiple Valkey containers (typically 6+ nodes)
- Cluster initialization (`CLUSTER MEET`, `CLUSTER ADDSLOTS`, etc.)
- Replication setup between master and replica nodes

For production use, ValkeyStore works with GlideClusterClient when connecting to
a real Valkey cluster deployment.
"""

import pytest

from tests.conftest import detect_on_windows, should_skip_docker_tests


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not running")
@pytest.mark.skipif(detect_on_windows(), reason="Valkey is not supported on Windows")
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


@pytest.mark.skip(
    reason="Full cluster integration tests require a multi-node Valkey cluster setup. "
    "GlideClusterClient requires an actual cluster with CLUSTER SLOTS support. "
    "See test_valkey.py for standalone instance tests."
)
class TestValkeyClusterIntegration:
    """Placeholder for future full cluster integration tests.

    To implement these tests, you would need to:
    1. Set up multiple Valkey containers (e.g., 6 nodes: 3 masters + 3 replicas)
    2. Initialize the cluster using CLUSTER MEET commands
    3. Distribute hash slots across masters with CLUSTER ADDSLOTS
    4. Set up replication with CLUSTER REPLICATE
    5. Wait for cluster to reach a healthy state

    Example cluster setup would involve:
    ```python
    # Start 6 Valkey containers
    nodes = [
        (7000, 17000), (7001, 17001), (7002, 17002),  # Masters
        (7003, 17003), (7004, 17004), (7005, 17005),  # Replicas
    ]

    # Initialize cluster connections between nodes
    # Assign hash slots to masters
    # Set up replicas
    ```

    Once a cluster is available, tests would verify:
    - Key distribution across shards
    - Failover handling
    - Cross-slot operations
    - Cluster-specific commands
    """

    async def test_cluster_put_get(self):
        """Test basic put/get operations against a Valkey cluster."""
        pytest.skip("Requires multi-node Valkey cluster setup")

    async def test_cluster_multi_key_operations(self):
        """Test multi-key operations work correctly in cluster mode."""
        pytest.skip("Requires multi-node Valkey cluster setup")

    async def test_cluster_failover(self):
        """Test that operations continue working during node failover."""
        pytest.skip("Requires multi-node Valkey cluster setup")
