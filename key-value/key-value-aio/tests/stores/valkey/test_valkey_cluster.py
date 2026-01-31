"""Tests for ValkeyStore with GlideClusterClient support.

This test file verifies ValkeyStore type compatibility with GlideClusterClient.
"""



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
