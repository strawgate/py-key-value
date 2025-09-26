import tempfile
from collections.abc import AsyncGenerator

import pytest
from typing_extensions import override

from kv_store_adapter.stores.disk import DiskStore
from tests.stores.conftest import BaseStoreTests, ContextManagerStoreTestMixin

TEST_SIZE_LIMIT = 100 * 1024  # 100KB


class TestDiskStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[DiskStore, None]:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = DiskStore(directory=temp_dir, max_size=TEST_SIZE_LIMIT)

            yield store
