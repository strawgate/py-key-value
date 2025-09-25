import tempfile
from collections.abc import AsyncGenerator

import pytest
from typing_extensions import override

from kv_store_adapter.stores.disk import DiskStore
from tests.stores.conftest import BaseStoreTests

TEST_SIZE_LIMIT = 1 * 1024 * 1024  # 1MB


class TestMemoryStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[DiskStore, None]:
        with tempfile.TemporaryDirectory() as temp_dir:
            yield DiskStore(directory=temp_dir, size_limit=TEST_SIZE_LIMIT)
