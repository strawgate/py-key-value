"""Tests for FileTreeStore."""

import tempfile
from collections.abc import AsyncGenerator
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
    async def store(self) -> AsyncGenerator[FileTreeStore, None]:
        """Create a FileTreeStore instance with a temporary directory.

        Uses V1 sanitization strategies to maintain backwards compatibility
        and pass tests that rely on sanitization for long/special names.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            yield FileTreeStore(
                data_directory=temp_path,
                key_sanitization_strategy=FileTreeV1KeySanitizationStrategy(directory=temp_path),
                collection_sanitization_strategy=FileTreeV1CollectionSanitizationStrategy(directory=temp_path),
            )

    @override
    async def test_not_unbounded(self, store: BaseStore):
        """FileTreeStore is unbounded, so skip this test."""
        pytest.skip("FileTreeStore is unbounded and does not evict old entries")
