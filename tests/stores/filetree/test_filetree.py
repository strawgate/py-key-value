"""Tests for FileTreeStore."""

import os
from pathlib import Path

import pytest
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.filetree import (
    FileTreeStore,
    FileTreeV1CollectionSanitizationStrategy,
    FileTreeV1KeySanitizationStrategy,
)
from key_value.shared.errors import PathSecurityError, StoreSetupError
from key_value.shared.sanitization import PassthroughStrategy
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


class TestFileTreeStorePathTraversal:
    """Test suite for FileTreeStore path traversal security."""

    @pytest.fixture
    def unsanitized_store(self, tmp_path: Path) -> FileTreeStore:
        """Create a FileTreeStore without sanitization strategies to test security."""
        return FileTreeStore(
            data_directory=tmp_path,
            key_sanitization_strategy=PassthroughStrategy(),
            collection_sanitization_strategy=PassthroughStrategy(),
        )

    async def test_path_traversal_in_key_blocked(self, unsanitized_store: FileTreeStore):
        """Test that path traversal in keys is blocked."""
        with pytest.raises(PathSecurityError):
            await unsanitized_store.put(
                collection="test",
                key="../../../../tmp/evil",
                value={"pwned": True},
            )

    async def test_path_traversal_in_collection_blocked(self, unsanitized_store: FileTreeStore):
        """Test that path traversal in collection names is blocked."""
        # Collection path traversal is caught during setup, which wraps in StoreSetupError
        with pytest.raises(StoreSetupError) as exc_info:
            await unsanitized_store.put(
                collection="../../../../tmp/evil",
                key="test_key",
                value={"pwned": True},
            )
        # Verify the underlying cause is PathSecurityError
        assert isinstance(exc_info.value.__cause__, PathSecurityError)

    async def test_path_traversal_get_blocked(self, unsanitized_store: FileTreeStore):
        """Test that path traversal in get operations is blocked."""
        with pytest.raises(PathSecurityError):
            await unsanitized_store.get(
                collection="test",
                key="../../../../etc/passwd",
            )

    async def test_path_traversal_delete_blocked(self, unsanitized_store: FileTreeStore):
        """Test that path traversal in delete operations is blocked."""
        with pytest.raises(PathSecurityError):
            await unsanitized_store.delete(
                collection="test",
                key="../../../../tmp/important_file",
            )


class TestFileTreeStoreSymlinkProtection:
    """Test suite for FileTreeStore symlink protection."""

    @pytest.fixture
    def data_dir(self, tmp_path: Path) -> Path:
        """Create a data directory for the store."""
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        return data_dir

    @pytest.fixture
    def unsanitized_store(self, data_dir: Path) -> FileTreeStore:
        """Create a FileTreeStore without sanitization strategies to test security."""
        return FileTreeStore(
            data_directory=data_dir,
            key_sanitization_strategy=PassthroughStrategy(),
            collection_sanitization_strategy=PassthroughStrategy(),
        )

    @pytest.mark.skipif(os.name == "nt", reason="Symlinks require elevated privileges on Windows")
    async def test_symlink_escape_blocked(self, unsanitized_store: FileTreeStore, data_dir: Path, tmp_path: Path):
        """Test that symlinks pointing outside the data directory are blocked."""
        # Create a directory outside the data directory
        external_dir = tmp_path / "external_target"
        external_dir.mkdir(parents=True, exist_ok=True)

        # Create a collection directory
        collection_dir = data_dir / "evil_collection"
        collection_dir.mkdir(parents=True, exist_ok=True)

        # Create a symlink inside the collection that points outside
        symlink_path = collection_dir / "escape_link"
        symlink_path.symlink_to(external_dir)

        # Attempt to write through the symlink should be blocked
        with pytest.raises(PathSecurityError):
            await unsanitized_store.put(
                collection="evil_collection",
                key="escape_link/pwned",
                value={"pwned": True},
            )

    @pytest.mark.skipif(os.name == "nt", reason="Symlinks require elevated privileges on Windows")
    async def test_symlink_within_directory_allowed(self, unsanitized_store: FileTreeStore, data_dir: Path):
        """Test that symlinks pointing within the data directory are allowed."""
        # Create a target directory within the data directory
        target_dir = data_dir / "target_collection"
        target_dir.mkdir(parents=True, exist_ok=True)

        # Create a symlink within data directory pointing to another dir in data directory
        link_collection = data_dir / "link_collection"
        link_collection.symlink_to(target_dir)

        # This should work since the symlink stays within the data directory
        await unsanitized_store.put(
            collection="link_collection",
            key="test_key",
            value={"data": "value"},
        )

        result = await unsanitized_store.get(collection="link_collection", key="test_key")
        assert result == {"data": "value"}


class TestFileTreeStoreAtomicWrites:
    """Test suite for FileTreeStore atomic write behavior."""

    @pytest.fixture
    def store(self, tmp_path: Path) -> FileTreeStore:
        """Create a FileTreeStore for testing atomic writes."""
        return FileTreeStore(
            data_directory=tmp_path,
            key_sanitization_strategy=FileTreeV1KeySanitizationStrategy(directory=tmp_path),
            collection_sanitization_strategy=FileTreeV1CollectionSanitizationStrategy(directory=tmp_path),
        )

    async def test_no_temp_files_left_after_write(self, store: FileTreeStore, tmp_path: Path):
        """Test that no temporary files are left after a successful write."""
        await store.put(collection="test", key="key1", value={"data": "value"})

        # Check that no .tmp files exist in the data directory
        tmp_files = list(tmp_path.rglob("*.tmp"))
        assert len(tmp_files) == 0, f"Found leftover temp files: {tmp_files}"

    async def test_file_content_is_complete(self, store: FileTreeStore):
        """Test that written files contain complete, valid data."""
        test_value = {"key": "value", "nested": {"data": [1, 2, 3]}}
        await store.put(collection="test", key="complete_test", value=test_value)

        result = await store.get(collection="test", key="complete_test")
        assert result == test_value

    async def test_overwrite_is_atomic(self, store: FileTreeStore):
        """Test that overwriting an existing key is atomic."""
        # Write initial value
        await store.put(collection="test", key="overwrite_test", value={"version": 1})

        # Overwrite with new value
        await store.put(collection="test", key="overwrite_test", value={"version": 2})

        # Verify the final value
        result = await store.get(collection="test", key="overwrite_test")
        assert result == {"version": 2}
