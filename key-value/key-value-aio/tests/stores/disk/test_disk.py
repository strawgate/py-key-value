import json
from pathlib import Path

import pytest
from dirty_equals import IsDatetime
from diskcache.core import Cache
from inline_snapshot import snapshot
from typing_extensions import override

from key_value.aio.stores.disk import DiskStore
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

TEST_SIZE_LIMIT = 100 * 1024  # 100KB


class TestDiskStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, per_test_temp_dir: Path) -> DiskStore:
        disk_store = DiskStore(directory=per_test_temp_dir, max_size=TEST_SIZE_LIMIT)

        disk_store._cache.clear()  # pyright: ignore[reportPrivateUsage]

        return disk_store

    @pytest.fixture
    async def disk_cache(self, store: DiskStore) -> Cache:
        assert isinstance(store._cache, Cache)
        return store._cache  # pyright: ignore[reportPrivateUsage]

    async def test_value_stored(self, store: DiskStore, disk_cache: Cache):
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        value = disk_cache.get(key="test::test_key")
        value_as_dict = json.loads(value)
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

        value = disk_cache.get(key="test::test_key")
        value_as_dict = json.loads(value)
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
