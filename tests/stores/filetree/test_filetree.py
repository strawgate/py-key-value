"""Tests for FileTreeStore."""

from pathlib import Path

import pytest
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.filetree import (
    FileTreeStore,
    FileTreeV1CollectionSanitizationStrategy,
    FileTreeV1KeySanitizationStrategy,
)
from tests.stores.base import BaseStoreTests


class TestFileTreeStore(BaseStoreTests):
    """Test suite for FileTreeStore."""

    @pytest.fixture
    async def store(self, per_test_temp_dir: Path) -> FileTreeStore:
        """Create a FileTreeStore instance with a temporary directory.

        Uses V1 sanitization strategies to maintain backwards compatibility
        and pass tests that rely on sanitization for long/special names.
        """
        return FileTreeStore(
            data_directory=per_test_temp_dir,
            key_sanitization_strategy=FileTreeV1KeySanitizationStrategy(directory=per_test_temp_dir),
            collection_sanitization_strategy=FileTreeV1CollectionSanitizationStrategy(directory=per_test_temp_dir),
        )

    @override
    async def test_not_unbounded(self, store: BaseStore):
        """FileTreeStore is unbounded, so skip this test."""
        pytest.skip("FileTreeStore is unbounded and does not evict old entries")
