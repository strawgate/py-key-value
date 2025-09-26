import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from typing_extensions import override

from kv_store_adapter.stores.disk.multi_store import MultiDiskStore
from tests.stores.conftest import BaseStoreTests

TEST_SIZE_LIMIT = 100 * 1024  # 100KB


class TestMultiDiskStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[MultiDiskStore, None]:
        with tempfile.TemporaryDirectory() as temp_dir:
            disk_store = MultiDiskStore(base_directory=Path(temp_dir), max_size=TEST_SIZE_LIMIT)

            yield disk_store
