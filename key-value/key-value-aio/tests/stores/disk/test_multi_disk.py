import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from typing_extensions import override

from key_value.aio.stores.disk.multi_store import MultiDiskStore
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

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
