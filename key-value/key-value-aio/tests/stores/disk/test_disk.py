import tempfile
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone

import pytest
from inline_snapshot import snapshot
from key_value.shared.utils.compound import compound_key
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.disk import DiskStore
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

TEST_SIZE_LIMIT = 100 * 1024  # 100KB


def test_managed_entry_serialization():
    """Test ManagedEntry serialization to JSON for Disk storage."""
    created_at = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    expires_at = created_at + timedelta(seconds=10)

    managed_entry = ManagedEntry(value={"test": "test"}, created_at=created_at, expires_at=expires_at)
    # DiskStore uses include_expiration=False
    json_str = managed_entry.to_json(include_expiration=False)

    assert json_str == snapshot('{"value": {"test": "test"}}')

    round_trip_managed_entry = ManagedEntry.from_json(json_str=json_str)

    assert round_trip_managed_entry.value == managed_entry.value


class TestDiskStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(scope="session")
    async def disk_store(self) -> AsyncGenerator[DiskStore, None]:
        with tempfile.TemporaryDirectory() as temp_dir:
            yield DiskStore(directory=temp_dir, max_size=TEST_SIZE_LIMIT)

    @override
    @pytest.fixture
    async def store(self, disk_store: DiskStore) -> DiskStore:
        disk_store._cache.clear()  # pyright: ignore[reportPrivateUsage]

        return disk_store

    async def test_value_stored_as_file(self, store: DiskStore):
        """Verify values are stored as JSON in files on disk."""
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        # Get the file path where the data is stored
        # DiskStore uses diskcache which hashes keys to create file paths
        # We need to access the raw file content
        combo_key = compound_key(collection="test", key="test_key")

        # Use the cache's internal method to get the value
        raw_value, _ = store._cache.get(key=combo_key, expire_time=True)  # pyright: ignore[reportPrivateUsage]

        assert raw_value == snapshot('{"value": {"name": "Alice", "age": 30}}')
