"""Tests for FileTreeStore."""

import asyncio
import os
from pathlib import Path
from typing import Any

import pytest
from anyio import Path as AsyncPath
from cryptography.fernet import Fernet
from typing_extensions import override

from key_value.aio._utils.sanitization import PassthroughStrategy
from key_value.aio.errors import PathSecurityError, StoreSetupError
from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.filetree import (
    FileTreeStore,
    FileTreeV1CollectionSanitizationStrategy,
    FileTreeV1KeySanitizationStrategy,
)
from key_value.aio.stores.filetree import store as filetree_store_module
from key_value.aio.wrappers.compression import CompressionWrapper
from key_value.aio.wrappers.encryption import FernetEncryptionWrapper
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

    async def test_delete_returns_false_when_file_disappears_before_unlink(
        self,
        store: FileTreeStore,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """delete should remain idempotent when another actor removes the file first."""
        await store.put(collection="test", key="race_key", value={"data": "value"})

        original_unlink = AsyncPath.unlink

        async def unlink_after_external_removal(path: AsyncPath) -> None:
            if Path(path).name == "race_key.json":
                Path(path).unlink()
                raise FileNotFoundError(Path(path))
            await original_unlink(path)

        monkeypatch.setattr(AsyncPath, "unlink", unlink_after_external_removal)

        assert await store.delete(collection="test", key="race_key") is False
        assert await store.get(collection="test", key="race_key") is None

    async def test_get_returns_none_when_file_disappears_before_read(
        self,
        store: FileTreeStore,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """get should return None, not raise, when another actor deletes the file before the read completes."""
        await store.put(collection="test", key="race_key", value={"data": "value"})

        original_read_file = filetree_store_module.read_file

        async def read_file_after_external_removal(file: AsyncPath) -> dict[str, Any]:
            if Path(file).name == "race_key.json":
                Path(file).unlink()
                raise FileNotFoundError(Path(file))
            return await original_read_file(file)

        monkeypatch.setattr(filetree_store_module, "read_file", read_file_after_external_removal)

        assert await store.get(collection="test", key="race_key") is None

    @staticmethod
    def _count_entry_files(data_directory: Path) -> int:
        return sum(1 for p in data_directory.rglob("*.json") if not p.name.endswith("-info.json"))

    async def _wait_until_expired(self, store: FileTreeStore, *, collection: str, key: str) -> None:
        for _ in range(8):
            await asyncio.sleep(0.25)
            if await store.ttl(collection=collection, key=key) == (None, None):
                return
        pytest.fail("entry never expired")

    async def test_cull_removes_expired_entries(self, store: FileTreeStore, per_test_temp_dir: Path):
        """cull() should delete expired, write-once-never-reread entries from disk, and only those."""
        await store.put(collection="test", key="short_lived", value={"data": "value"}, ttl=1)
        await store.put(collection="test", key="long_lived", value={"data": "value"})
        assert self._count_entry_files(per_test_temp_dir) == 2

        await self._wait_until_expired(store, collection="test", key="short_lived")

        # Expiry is only honored on read; the file is still on disk until cull() runs.
        assert self._count_entry_files(per_test_temp_dir) == 2

        await store.cull()

        assert self._count_entry_files(per_test_temp_dir) == 1
        assert await store.get(collection="test", key="long_lived") == {"data": "value"}

    async def test_cull_does_not_depend_on_collection_metadata_index(self, store: FileTreeStore, per_test_temp_dir: Path):
        """cull() must find expired entries by walking disk, not by trusting the collection metadata index.

        The index only ever lists collections it knows about; deleting it (or a fresh process never having
        seen it) does not mean the underlying key files are gone.
        """
        await store.put(collection="test", key="short_lived", value={"data": "value"}, ttl=1)
        await self._wait_until_expired(store, collection="test", key="short_lived")

        info_files = list(per_test_temp_dir.glob("*-info.json"))
        assert info_files, "expected a collection info file to exist before removing it"
        for info_file in info_files:
            info_file.unlink()

        assert self._count_entry_files(per_test_temp_dir) == 1

        await store.cull()

        assert self._count_entry_files(per_test_temp_dir) == 0

    async def test_cull_works_through_encryption_and_compression_wrappers(self, store: FileTreeStore, per_test_temp_dir: Path):
        """cull() reads expires_at directly off disk, so it must not care that `value` is encrypted/compressed.

        Wrappers only transform the `value` payload before it reaches the store; created_at/expires_at are
        always written in plaintext by the store's own ManagedEntry/serialization layer.
        """
        wrapped = CompressionWrapper(
            key_value=FernetEncryptionWrapper(key_value=store, fernet=Fernet(key=Fernet.generate_key())),
            min_size_to_compress=1,
        )

        await wrapped.put(collection="test", key="short_lived", value={"data": "x" * 2048}, ttl=1)
        assert self._count_entry_files(per_test_temp_dir) == 1

        await self._wait_until_expired(store, collection="test", key="short_lived")
        assert self._count_entry_files(per_test_temp_dir) == 1

        # cull() is an optional capability that wrappers don't proxy, so it's called on the underlying store.
        await store.cull()

        assert self._count_entry_files(per_test_temp_dir) == 0

    async def test_cull_discovers_collections_untouched_by_this_instance(self, per_test_temp_dir: Path):
        """cull() should reclaim space for collections this store instance has never read or written."""
        writer = FileTreeStore(
            data_directory=per_test_temp_dir,
            key_sanitization_strategy=FileTreeV1KeySanitizationStrategy(directory=per_test_temp_dir),
            collection_sanitization_strategy=FileTreeV1CollectionSanitizationStrategy(directory=per_test_temp_dir),
        )
        await writer.put(collection="test", key="short_lived", value={"data": "value"}, ttl=1)
        await self._wait_until_expired(writer, collection="test", key="short_lived")
        assert self._count_entry_files(per_test_temp_dir) == 1

        reader = FileTreeStore(
            data_directory=per_test_temp_dir,
            key_sanitization_strategy=FileTreeV1KeySanitizationStrategy(directory=per_test_temp_dir),
            collection_sanitization_strategy=FileTreeV1CollectionSanitizationStrategy(directory=per_test_temp_dir),
        )
        await reader.cull()

        assert self._count_entry_files(per_test_temp_dir) == 0

    async def test_cull_removes_expired_entry_named_info(self, store: FileTreeStore, per_test_temp_dir: Path):
        """A key literally named "info" is a normal key file, not collection metadata, so cull() must reap it too."""
        await store.put(collection="test", key="info", value={"data": "value"}, ttl=1)
        await self._wait_until_expired(store, collection="test", key="info")
        assert self._count_entry_files(per_test_temp_dir) == 1

        await store.cull()

        assert self._count_entry_files(per_test_temp_dir) == 0

    @pytest.mark.skipif(os.name == "nt", reason="Symlinks require elevated privileges on Windows")
    async def test_cull_skips_symlinked_collection_directory_outside_root(
        self, store: FileTreeStore, per_test_temp_dir: Path, tmp_path: Path
    ):
        """cull() must not follow a directory symlink under the data directory to reach files outside the store root."""
        external_dir = tmp_path / "external"
        external_dir.mkdir()
        external_file = external_dir / "leaked.json"
        external_file.write_text('{"version": 1, "value": {"secret": "data"}, "expires_at": "2000-01-01T00:00:00+00:00"}')

        (per_test_temp_dir / "escape_link").symlink_to(external_dir)

        await store.cull()

        assert external_file.exists()

    async def test_cull_does_not_delete_concurrently_replaced_entry(
        self, store: FileTreeStore, per_test_temp_dir: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """If a put() replaces an expired file between cull()'s read and its delete check, the fresh value must survive."""
        await store.put(collection="test", key="racy", value={"data": "stale"}, ttl=1)
        await self._wait_until_expired(store, collection="test", key="racy")

        original_stat = AsyncPath.stat
        stat_calls_by_name: dict[str, int] = {}

        async def stat_with_concurrent_replace(self_path: AsyncPath) -> os.stat_result:
            name = Path(self_path).name
            stat_calls_by_name[name] = stat_calls_by_name.get(name, 0) + 1
            if name.startswith("racy") and stat_calls_by_name[name] == 2:
                # cull() already read the stale value and is about to re-check the file before
                # deleting it (its second stat() call on this path) -- replace it right now, as a
                # concurrent put() landing in that exact window would.
                await store.put(collection="test", key="racy", value={"data": "fresh"}, ttl=100)
            return await original_stat(self_path)

        monkeypatch.setattr(AsyncPath, "stat", stat_with_concurrent_replace)

        await store.cull()

        assert await store.get(collection="test", key="racy") == {"data": "fresh"}

    async def test_cull_skips_unparseable_entry_and_continues(self, store: FileTreeStore, per_test_temp_dir: Path):
        """A single malformed .json file in a collection must not stop cull() from reaping other expired entries."""
        await store.put(collection="test", key="short_lived", value={"data": "value"}, ttl=1)
        await self._wait_until_expired(store, collection="test", key="short_lived")

        collection_dirs = [p for p in per_test_temp_dir.iterdir() if p.is_dir()]
        assert len(collection_dirs) == 1
        (collection_dirs[0] / "corrupt.json").write_text('{"not": "a managed entry"}')

        await store.cull()

        assert (collection_dirs[0] / "corrupt.json").exists()
        assert self._count_entry_files(per_test_temp_dir) == 1


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
