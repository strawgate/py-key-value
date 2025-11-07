import json
import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from dirty_equals import IsDatetime
from inline_snapshot import snapshot
from typing_extensions import override

from key_value.aio.stores.disk.multi_store import MultiDiskStore
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

if TYPE_CHECKING:
    from diskcache.core import Cache

TEST_SIZE_LIMIT = 100 * 1024  # 100KB


class TestMultiDiskStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(scope="session")
    async def multi_disk_store(self) -> AsyncGenerator[MultiDiskStore, None]:
        with tempfile.TemporaryDirectory() as temp_dir:
            yield MultiDiskStore(base_directory=Path(temp_dir), max_size=TEST_SIZE_LIMIT)

    @override
    @pytest.fixture
    async def store(self, multi_disk_store: MultiDiskStore) -> MultiDiskStore:
        for collection in multi_disk_store._cache:  # pyright: ignore[reportPrivateUsage]
            multi_disk_store._cache[collection].clear()  # pyright: ignore[reportPrivateUsage]

        return multi_disk_store

    async def test_value_stored(self, store: MultiDiskStore):
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})
        disk_cache: Cache = store._cache["test"]  # pyright: ignore[reportPrivateUsage]

        value = disk_cache.get(key="test_key")
        value_as_dict = json.loads(value)
        assert value_as_dict == snapshot(
            {
                "collection": "test",
                "value": {"name": "Alice", "age": 30},
                "key": "test_key",
                "created_at": IsDatetime(iso_string=True),
                "version": 1,
            }
        )

        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30}, ttl=10)

        value = disk_cache.get(key="test_key")
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
