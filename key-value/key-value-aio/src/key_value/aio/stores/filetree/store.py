"""FileTreeStore implementation using native filesystem operations."""

from pathlib import Path

from key_value.shared.errors import DeserializationError
from key_value.shared.utils.managed_entry import ManagedEntry
from key_value.shared.utils.serialization import BasicSerializationAdapter, SerializationAdapter
from typing_extensions import override

from key_value.aio.stores.base import (
    BaseDestroyCollectionStore,
    BaseDestroyStore,
    BaseEnumerateCollectionsStore,
    BaseEnumerateKeysStore,
)

DEFAULT_PAGE_SIZE = 10000
PAGE_LIMIT = 10000


class FileTreeStore(BaseDestroyStore, BaseDestroyCollectionStore, BaseEnumerateCollectionsStore, BaseEnumerateKeysStore):
    """A file-tree based store using directories for collections and files for keys.

    This store uses the native filesystem:
    - Each collection is a subdirectory under the base directory
    - Each key is stored as a JSON file named "{key}.json"
    - File contents contain the ManagedEntry serialized to JSON

    Directory structure:
        {base_directory}/
            {collection_1}/
                {key_1}.json
                {key_2}.json
            {collection_2}/
                {key_3}.json

    Warning:
        This store is intended for development and testing purposes only.
        It is not suitable for production use due to:
        - Poor performance with many keys
        - No atomic operations
        - No built-in cleanup of expired entries
        - Filesystem limitations on file names and directory sizes

    The store does NOT automatically clean up expired entries from disk. Expired entries
    are only filtered out when read via get() or similar methods.
    """

    _directory: Path

    def __init__(
        self,
        *,
        directory: Path | str,
        serialization_adapter: SerializationAdapter | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the file-tree store.

        Args:
            directory: The base directory to use for storing collections and keys.
            serialization_adapter: The serialization adapter to use for the store.
            default_collection: The default collection to use if no collection is provided.
        """
        self._directory = Path(directory).resolve()
        self._directory.mkdir(parents=True, exist_ok=True)

        self._stable_api = False

        super().__init__(
            serialization_adapter=serialization_adapter or BasicSerializationAdapter(),
            default_collection=default_collection,
        )

    def _get_collection_path(self, collection: str) -> Path:
        """Get the path to a collection directory.

        Args:
            collection: The collection name.

        Returns:
            The path to the collection directory.

        Raises:
            ValueError: If the collection name would result in a path outside the base directory.
        """
        collection_path = (self._directory / collection).resolve()

        if not collection_path.is_relative_to(self._directory):
            msg = f"Invalid collection name: {collection!r} would escape base directory"
            raise ValueError(msg)

        return collection_path

    def _get_key_path(self, collection: str, key: str) -> Path:
        """Get the path to a key file.

        Args:
            collection: The collection name.
            key: The key name.

        Returns:
            The path to the key file.

        Raises:
            ValueError: If the collection or key name would result in a path outside the base directory.
        """
        collection_path = self._get_collection_path(collection)
        key_path = (collection_path / f"{key}.json").resolve()

        if not key_path.is_relative_to(self._directory):
            msg = f"Invalid key name: {key!r} would escape base directory"
            raise ValueError(msg)

        return key_path

    @override
    async def _setup_collection(self, *, collection: str) -> None:
        """Set up a collection by creating its directory if it doesn't exist.

        Args:
            collection: The collection name.
        """
        collection_path = self._get_collection_path(collection)
        collection_path.mkdir(parents=True, exist_ok=True)

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        """Retrieve a managed entry by key from the specified collection.

        Args:
            collection: The collection name.
            key: The key name.

        Returns:
            The managed entry if found and not expired, None otherwise.
        """
        key_path = self._get_key_path(collection, key)

        if not key_path.exists():
            return None

        try:
            json_str = key_path.read_text(encoding="utf-8")
            return self._serialization_adapter.load_json(json_str=json_str)
        except (OSError, DeserializationError):
            # If we can't read or parse the file, treat it as not found
            return None

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        """Store a managed entry at the specified key in the collection.

        Args:
            collection: The collection name.
            key: The key name.
            managed_entry: The managed entry to store.
        """
        key_path = self._get_key_path(collection, key)

        # Ensure the parent directory exists
        key_path.parent.mkdir(parents=True, exist_ok=True)

        # Write the managed entry to the file
        json_str = self._serialization_adapter.dump_json(entry=managed_entry)
        key_path.write_text(json_str, encoding="utf-8")

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        """Delete a managed entry from the specified collection.

        Args:
            collection: The collection name.
            key: The key name.

        Returns:
            True if the entry was deleted, False if it didn't exist.
        """
        key_path = self._get_key_path(collection, key)

        if not key_path.exists():
            return False

        try:
            key_path.unlink()
        except OSError:
            return False
        else:
            return True

    @override
    async def _get_collection_keys(self, *, collection: str, limit: int | None = None) -> list[str]:
        """List all keys in the specified collection.

        Args:
            collection: The collection name.
            limit: Maximum number of keys to return.

        Returns:
            A list of key names (without the .json extension).
        """
        limit = min(limit or DEFAULT_PAGE_SIZE, PAGE_LIMIT)
        collection_path = self._get_collection_path(collection)

        if not collection_path.exists():
            return []

        keys: list[str] = []
        for file_path in collection_path.iterdir():
            if file_path.is_file() and file_path.suffix == ".json":
                keys.append(file_path.stem)
                if len(keys) >= limit:
                    break

        return keys

    @override
    async def _get_collection_names(self, *, limit: int | None = None) -> list[str]:
        """List all collection names.

        Args:
            limit: Maximum number of collections to return.

        Returns:
            A list of collection names.
        """
        limit = min(limit or DEFAULT_PAGE_SIZE, PAGE_LIMIT)

        collections: list[str] = []
        for dir_path in self._directory.iterdir():
            if dir_path.is_dir():
                collections.append(dir_path.name)
                if len(collections) >= limit:
                    break

        return collections

    @override
    async def _delete_collection(self, *, collection: str) -> bool:
        """Delete an entire collection and all its keys.

        Args:
            collection: The collection name.

        Returns:
            True if the collection was deleted, False if it didn't exist or an error occurred.
        """
        collection_path = self._get_collection_path(collection)

        if not collection_path.exists():
            return False

        try:
            # Delete all files in the collection
            for file_path in collection_path.iterdir():
                if file_path.is_file():
                    file_path.unlink()

            # Delete the collection directory
            collection_path.rmdir()
        except OSError:
            return False
        else:
            return True

    @override
    async def _delete_store(self) -> bool:
        """Delete the entire store and all its collections.

        Returns:
            True if the store was deleted successfully.
        """
        try:
            # Delete all collections
            for collection_path in self._directory.iterdir():
                if collection_path.is_dir():
                    # Delete all files in the collection
                    for file_path in collection_path.iterdir():
                        if file_path.is_file():
                            file_path.unlink()
                    # Delete the collection directory
                    collection_path.rmdir()
        except OSError:
            return False
        else:
            return True
