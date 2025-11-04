"""Tests for FileTreeStore."""

import tempfile
from collections.abc import AsyncGenerator

import pytest
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.filetree import FileTreeStore
from tests.stores.base import BaseStoreTests


class TestFileTreeStore(BaseStoreTests):
    """Test suite for FileTreeStore."""

    @pytest.fixture
    async def store(self) -> AsyncGenerator[FileTreeStore, None]:
        """Create a FileTreeStore instance with a temporary directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield FileTreeStore(data_directory=temp_dir)

    @override
    async def test_not_unbounded(self, store: BaseStore):
        """FileTreeStore is unbounded, so skip this test."""
        pytest.skip("FileTreeStore is unbounded and does not evict old entries")
