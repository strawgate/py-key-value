import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from typing_extensions import override

from kv_store_adapter.stores.disk.multi_store import MultiDiskStore
from tests.stores.conftest import BaseStoreTests

TEST_SIZE_LIMIT = 1 * 1024 * 1024  # 1MB


class TestMultiDiskStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[MultiDiskStore, None]:
        with tempfile.TemporaryDirectory() as temp_dir:
            yield MultiDiskStore(base_directory=Path(temp_dir), max_size=TEST_SIZE_LIMIT)
