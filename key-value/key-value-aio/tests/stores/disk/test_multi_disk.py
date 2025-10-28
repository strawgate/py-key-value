import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from typing_extensions import override

from key_value.aio.stores.disk.multi_store import MultiDiskStore
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

TEST_SIZE_LIMIT = 100 * 1024  # 100KB


class TestMultiDiskStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[MultiDiskStore, None]:
        with tempfile.TemporaryDirectory() as temp_dir:
            yield MultiDiskStore(base_directory=Path(temp_dir), max_size=TEST_SIZE_LIMIT)
