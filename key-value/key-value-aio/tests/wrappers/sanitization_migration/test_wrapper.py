"""Tests for SanitizationMigrationWrapper."""

import pytest

from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.sanitization_migration import SanitizationMigrationWrapper


class TestSanitizationMigrationWrapper:
    """Tests for SanitizationMigrationWrapper."""

    @pytest.fixture
    def current_store(self) -> MemoryStore:
        """Create a current store (new strategy)."""
        return MemoryStore()

    @pytest.fixture
    def legacy_store(self) -> MemoryStore:
        """Create a legacy store (old strategy)."""
        return MemoryStore()

    @pytest.fixture
    def wrapper(self, current_store: MemoryStore, legacy_store: MemoryStore) -> SanitizationMigrationWrapper:
        """Create a migration wrapper."""
        return SanitizationMigrationWrapper(
            current_store=current_store,
            legacy_store=legacy_store,
            migrate_on_read=False,
            cache_size=100,
        )

    @pytest.fixture
    def migrating_wrapper(self, current_store: MemoryStore, legacy_store: MemoryStore) -> SanitizationMigrationWrapper:
        """Create a migration wrapper with migrate_on_read=True."""
        return SanitizationMigrationWrapper(
            current_store=current_store,
            legacy_store=legacy_store,
            migrate_on_read=True,
            delete_after_migration=False,
            cache_size=100,
        )

    async def test_get_from_current_store(self, wrapper: SanitizationMigrationWrapper, current_store: MemoryStore) -> None:
        """Test getting a value from the current store."""
        await current_store.put(key="test_key", value={"data": "current"}, collection="default")

        result = await wrapper.get(key="test_key", collection="default")
        assert result is not None
        assert result["data"] == "current"

    async def test_get_from_legacy_store(
        self, wrapper: SanitizationMigrationWrapper, legacy_store: MemoryStore, current_store: MemoryStore
    ) -> None:
        """Test getting a value from the legacy store when not in current."""
        await legacy_store.put(key="test_key", value={"data": "legacy"}, collection="default")

        result = await wrapper.get(key="test_key", collection="default")
        assert result is not None
        assert result["data"] == "legacy"

        # Should NOT have been migrated (migrate_on_read=False)
        current_result = await current_store.get(key="test_key", collection="default")
        assert current_result is None

    async def test_get_missing_key(self, wrapper: SanitizationMigrationWrapper) -> None:
        """Test getting a missing key returns None."""
        result = await wrapper.get(key="missing_key", collection="default")
        assert result is None

    async def test_migrate_on_read(
        self, migrating_wrapper: SanitizationMigrationWrapper, legacy_store: MemoryStore, current_store: MemoryStore
    ) -> None:
        """Test that migrate_on_read copies data from legacy to current."""
        await legacy_store.put(key="test_key", value={"data": "legacy"}, collection="default", ttl=3600)

        result = await migrating_wrapper.get(key="test_key", collection="default")
        assert result is not None
        assert result["data"] == "legacy"

        # Should have been migrated
        current_result = await current_store.get(key="test_key", collection="default")
        assert current_result is not None
        assert current_result["data"] == "legacy"

        # Legacy should still have it (delete_after_migration=False)
        legacy_result = await legacy_store.get(key="test_key", collection="default")
        assert legacy_result is not None

    async def test_migrate_on_read_with_delete(self, legacy_store: MemoryStore, current_store: MemoryStore) -> None:
        """Test that delete_after_migration removes from legacy."""
        wrapper = SanitizationMigrationWrapper(
            current_store=current_store,
            legacy_store=legacy_store,
            migrate_on_read=True,
            delete_after_migration=True,
            cache_size=100,
        )

        await legacy_store.put(key="test_key", value={"data": "legacy"}, collection="default")

        result = await wrapper.get(key="test_key", collection="default")
        assert result is not None

        # Should have been deleted from legacy
        legacy_result = await legacy_store.get(key="test_key", collection="default")
        assert legacy_result is None

    async def test_cache_avoids_repeated_lookups(
        self, wrapper: SanitizationMigrationWrapper, current_store: MemoryStore, legacy_store: MemoryStore
    ) -> None:
        """Test that cache avoids repeated lookups."""
        await current_store.put(key="test_key", value={"data": "current"}, collection="default")

        # First get - should cache
        result1 = await wrapper.get(key="test_key", collection="default")
        assert result1 is not None

        # Check cache
        cached_location = wrapper._cache_get(key="test_key", collection="default")  # pyright: ignore[reportPrivateUsage]
        assert cached_location == "current"

        # Second get - should use cache (wouldn't check legacy even if we added to it)
        await legacy_store.put(key="test_key", value={"data": "legacy"}, collection="default")
        result2 = await wrapper.get(key="test_key", collection="default")
        assert result2 is not None
        assert result2["data"] == "current"  # Still from current, not legacy

    async def test_cache_missing_keys(self, wrapper: SanitizationMigrationWrapper) -> None:
        """Test that missing keys are cached."""
        # First get - should cache as missing
        result1 = await wrapper.get(key="missing_key", collection="default")
        assert result1 is None

        # Check cache
        cached_location = wrapper._cache_get(key="missing_key", collection="default")  # pyright: ignore[reportPrivateUsage]
        assert cached_location == "missing"

        # Second get - should use cache
        result2 = await wrapper.get(key="missing_key", collection="default")
        assert result2 is None

    async def test_put_updates_cache(
        self, wrapper: SanitizationMigrationWrapper, current_store: MemoryStore, legacy_store: MemoryStore
    ) -> None:
        """Test that put updates the cache."""
        # Put initially in legacy
        await legacy_store.put(key="test_key", value={"data": "legacy"}, collection="default")

        # Get - should cache as legacy
        await wrapper.get(key="test_key", collection="default")
        assert wrapper._cache_get(key="test_key", collection="default") == "legacy"  # pyright: ignore[reportPrivateUsage]

        # Put - should update cache to current
        await wrapper.put(key="test_key", value={"data": "new"}, collection="default")
        assert wrapper._cache_get(key="test_key", collection="default") == "current"  # pyright: ignore[reportPrivateUsage]

        # Get - should get from current, not legacy
        result = await wrapper.get(key="test_key", collection="default")
        assert result is not None
        assert result["data"] == "new"

    async def test_delete_from_both_stores(
        self, wrapper: SanitizationMigrationWrapper, current_store: MemoryStore, legacy_store: MemoryStore
    ) -> None:
        """Test that delete removes from both stores."""
        await current_store.put(key="key1", value={"data": "current"}, collection="default")
        await legacy_store.put(key="key2", value={"data": "legacy"}, collection="default")

        # Delete should remove from both
        deleted1 = await wrapper.delete(key="key1", collection="default")
        deleted2 = await wrapper.delete(key="key2", collection="default")

        assert deleted1 is True
        assert deleted2 is True

        # Verify deletion
        assert await current_store.get(key="key1", collection="default") is None
        assert await legacy_store.get(key="key2", collection="default") is None

    async def test_get_many(self, wrapper: SanitizationMigrationWrapper, current_store: MemoryStore, legacy_store: MemoryStore) -> None:
        """Test get_many with keys in different stores."""
        await current_store.put(key="key1", value={"data": "current1"}, collection="default")
        await legacy_store.put(key="key2", value={"data": "legacy2"}, collection="default")

        results = await wrapper.get_many(keys=["key1", "key2", "key3"], collection="default")

        assert len(results) == 3
        assert results[0] is not None
        assert results[0]["data"] == "current1"
        assert results[1] is not None
        assert results[1]["data"] == "legacy2"
        assert results[2] is None

    async def test_get_many_with_migration(
        self, migrating_wrapper: SanitizationMigrationWrapper, legacy_store: MemoryStore, current_store: MemoryStore
    ) -> None:
        """Test get_many migrates keys from legacy to current."""
        await legacy_store.put(key="key1", value={"data": "legacy1"}, collection="default")
        await legacy_store.put(key="key2", value={"data": "legacy2"}, collection="default")

        results = await migrating_wrapper.get_many(keys=["key1", "key2"], collection="default")

        assert len(results) == 2
        assert results[0] is not None
        assert results[1] is not None

        # Both should have been migrated
        current_result1 = await current_store.get(key="key1", collection="default")
        current_result2 = await current_store.get(key="key2", collection="default")
        assert current_result1 is not None
        assert current_result2 is not None

    async def test_ttl_from_current(self, wrapper: SanitizationMigrationWrapper, current_store: MemoryStore) -> None:
        """Test ttl from current store."""
        await current_store.put(key="test_key", value={"data": "current"}, collection="default", ttl=3600)

        value, ttl = await wrapper.ttl(key="test_key", collection="default")
        assert value is not None
        assert value["data"] == "current"
        assert ttl is not None
        assert ttl > 0

    async def test_ttl_from_legacy(
        self, wrapper: SanitizationMigrationWrapper, legacy_store: MemoryStore, current_store: MemoryStore
    ) -> None:
        """Test ttl from legacy store."""
        await legacy_store.put(key="test_key", value={"data": "legacy"}, collection="default", ttl=3600)

        value, ttl = await wrapper.ttl(key="test_key", collection="default")
        assert value is not None
        assert value["data"] == "legacy"
        assert ttl is not None

        # Should NOT have been migrated (migrate_on_read=False)
        current_result = await current_store.get(key="test_key", collection="default")
        assert current_result is None

    async def test_ttl_with_migration(
        self, migrating_wrapper: SanitizationMigrationWrapper, legacy_store: MemoryStore, current_store: MemoryStore
    ) -> None:
        """Test ttl migrates from legacy to current."""
        await legacy_store.put(key="test_key", value={"data": "legacy"}, collection="default", ttl=3600)

        value, ttl = await migrating_wrapper.ttl(key="test_key", collection="default")
        assert value is not None
        assert ttl is not None

        # Should have been migrated with TTL preserved
        current_value, current_ttl = await current_store.ttl(key="test_key", collection="default")
        assert current_value is not None
        assert current_ttl is not None

    async def test_put_many(self, wrapper: SanitizationMigrationWrapper, current_store: MemoryStore) -> None:
        """Test put_many writes to current store."""
        await wrapper.put_many(
            keys=["key1", "key2"],
            values=[{"data": "value1"}, {"data": "value2"}],
            collection="default",
        )

        # Verify in current store
        result1 = await current_store.get(key="key1", collection="default")
        result2 = await current_store.get(key="key2", collection="default")
        assert result1 is not None
        assert result2 is not None

    async def test_delete_many(self, wrapper: SanitizationMigrationWrapper, current_store: MemoryStore, legacy_store: MemoryStore) -> None:
        """Test delete_many removes from both stores."""
        await current_store.put(key="key1", value={"data": "current"}, collection="default")
        await legacy_store.put(key="key2", value={"data": "legacy"}, collection="default")

        count = await wrapper.delete_many(keys=["key1", "key2"], collection="default")
        assert count == 2

        # Verify deletion
        assert await current_store.get(key="key1", collection="default") is None
        assert await legacy_store.get(key="key2", collection="default") is None

    async def test_cache_disabled(self, current_store: MemoryStore, legacy_store: MemoryStore) -> None:
        """Test wrapper with caching disabled."""
        wrapper = SanitizationMigrationWrapper(
            current_store=current_store,
            legacy_store=legacy_store,
            cache_size=0,  # Disable cache
        )

        await current_store.put(key="test_key", value={"data": "current"}, collection="default")

        result = await wrapper.get(key="test_key", collection="default")
        assert result is not None

        # Cache should be disabled
        assert not wrapper._cache_enabled  # pyright: ignore[reportPrivateUsage]

    async def test_ttl_many(self, wrapper: SanitizationMigrationWrapper, current_store: MemoryStore, legacy_store: MemoryStore) -> None:
        """Test ttl_many with keys in different stores."""
        await current_store.put(key="key1", value={"data": "current1"}, collection="default", ttl=3600)
        await legacy_store.put(key="key2", value={"data": "legacy2"}, collection="default", ttl=7200)

        results = await wrapper.ttl_many(keys=["key1", "key2", "key3"], collection="default")

        assert len(results) == 3
        assert results[0][0] is not None  # key1 value
        assert results[0][1] is not None  # key1 ttl
        assert results[1][0] is not None  # key2 value
        assert results[1][1] is not None  # key2 ttl
        assert results[2][0] is None  # key3 missing
        assert results[2][1] is None  # key3 no ttl
