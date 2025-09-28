import tempfile
from collections.abc import AsyncGenerator

import pytest
from typing_extensions import override

from key_value.aio.stores.disk import DiskStore
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

TEST_SIZE_LIMIT = 100 * 1024  # 100KB


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
