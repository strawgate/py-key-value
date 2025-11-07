"""FileTreeStore implementation using async filesystem operations."""

import os
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from aiofile import async_open as aopen
from anyio import Path as AsyncPath
from key_value.shared.utils.managed_entry import ManagedEntry, dump_to_json, load_from_json
from key_value.shared.utils.sanitization import HybridSanitizationStrategy, SanitizationStrategy
from key_value.shared.utils.sanitize import ALPHANUMERIC_CHARACTERS
from key_value.shared.utils.serialization import BasicSerializationAdapter, SerializationAdapter
from key_value.shared.utils.time_to_live import now
from typing_extensions import Self, override

from key_value.aio.stores.base import (
    BaseStore,
)

DEFAULT_PAGE_SIZE = 10000
PAGE_LIMIT = 10000

MAX_DIRECTORY_LENGTH = 255
DIRECTORY_ALLOWED_CHARACTERS = ALPHANUMERIC_CHARACTERS + "_"

MAX_FILE_NAME_LENGTH = 255
FILE_NAME_ALLOWED_CHARACTERS = ALPHANUMERIC_CHARACTERS + "_"


def get_max_path_length(root: Path | AsyncPath) -> int:
    return os.pathconf(path=Path(root), name="PC_PATH_MAX")


def get_max_file_name_length(root: Path | AsyncPath) -> int:
    return os.pathconf(path=Path(root), name="PC_NAME_MAX")


class FileTreeV1CollectionSanitizationStrategy(HybridSanitizationStrategy):
    """V1 sanitization strategy for FileTreeStore collections.

    This strategy sanitizes collection names to comply with filesystem directory naming requirements.
    It replaces invalid characters with underscores and truncates to fit within directory name length limits.

    Collection names (directories) are subject to the same length limit as file names (typically 255 bytes).
    The sanitized name is also used for the collection info file (with `-info.json` suffix), so we need
    to leave room for that suffix (10 characters).
    """

    def __init__(self, directory: Path | AsyncPath) -> None:
        # Directory names are subject to the same NAME_MAX limit as file names
        max_name_length: int = get_max_file_name_length(root=directory)

        # Leave room for `-info.json` suffix (10 chars) that's added to the metadata file name
        suffix_length = 10

        super().__init__(
            replacement_character="_",
            max_length=max_name_length - suffix_length,
            allowed_characters=DIRECTORY_ALLOWED_CHARACTERS,
        )


class FileTreeV1KeySanitizationStrategy(HybridSanitizationStrategy):
    """V1 sanitization strategy for FileTreeStore keys.

    This strategy sanitizes key names to comply with filesystem file naming requirements.
    It replaces invalid characters with underscores and truncates to fit within both path
    length limits and filename length limits.
    """

    def __init__(self, directory: Path | AsyncPath) -> None:
        # We need to account for our current location in the filesystem to stay under the max path length
        max_path_length: int = get_max_path_length(root=directory)
        current_path_length: int = len(Path(directory).as_posix())
        remaining_length: int = max_path_length - current_path_length

        # We need to account for limits on file names
        max_file_name_length: int = get_max_file_name_length(root=directory) - 5  # 5 for .json extension

        # We need to stay under both limits
        max_length = min(remaining_length, max_file_name_length)

        super().__init__(
            replacement_character="_",
            max_length=max_length,
            allowed_characters=FILE_NAME_ALLOWED_CHARACTERS,
        )


@dataclass(kw_only=True)
class DiskCollectionInfo:
    version: int = 1

    collection: str

    directory: AsyncPath

    created_at: datetime

    serialization_adapter: SerializationAdapter
    key_sanitization_strategy: SanitizationStrategy

    async def _list_file_paths(self) -> AsyncGenerator[AsyncPath]:
        async for item_path in AsyncPath(self.directory).iterdir():
            if not await item_path.is_file() or item_path.suffix != ".json":
                continue
            if item_path.stem == "info":
                continue
            yield item_path

    async def get_entry(self, *, key: str) -> ManagedEntry | None:
        sanitized_key = self.key_sanitization_strategy.sanitize(value=key)
        key_path: AsyncPath = AsyncPath(self.directory / f"{sanitized_key}.json")

        if not await key_path.exists():
            return None

        data_dict: dict[str, Any] = await read_file(file=key_path)

        return self.serialization_adapter.load_dict(data=data_dict)

    async def gen_entries(self) -> AsyncGenerator[ManagedEntry]:
        async for key_path in self._list_file_paths():
            yield self.serialization_adapter.load_dict(data=await read_file(file=key_path))

    async def put_entry(self, *, key: str, data: ManagedEntry) -> None:
        sanitized_key = self.key_sanitization_strategy.sanitize(value=key)
        key_path: AsyncPath = AsyncPath(self.directory / f"{sanitized_key}.json")
        await write_file(file=key_path, text=self.serialization_adapter.dump_json(entry=data))

    async def delete_entry(self, *, key: str) -> bool:
        sanitized_key = self.key_sanitization_strategy.sanitize(value=key)
        key_path: AsyncPath = AsyncPath(self.directory / f"{sanitized_key}.json")

        if not await key_path.exists():
            return False

        await key_path.unlink()

        return True

    # async def delete_all_entries(self) -> None:
    #     async for entry_path in self._list_file_paths():
    #         await entry_path.unlink()

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "collection": self.collection,
            "directory": str(self.directory),
            "created_at": self.created_at.isoformat(),
        }

    def to_json(self) -> str:
        return dump_to_json(obj=self.to_dict())

    @classmethod
    def from_dict(
        cls, *, data: dict[str, Any], serialization_adapter: SerializationAdapter, key_sanitization_strategy: SanitizationStrategy
    ) -> Self:
        return cls(
            version=data["version"],
            collection=data["collection"],
            directory=AsyncPath(data["directory"]),
            created_at=datetime.fromisoformat(data["created_at"]),
            serialization_adapter=serialization_adapter,
            key_sanitization_strategy=key_sanitization_strategy,
        )

    @classmethod
    async def from_file(
        cls, *, file: AsyncPath, serialization_adapter: SerializationAdapter, key_sanitization_strategy: SanitizationStrategy
    ) -> Self:
        if data := await read_file(file=file):
            data["directory"] = AsyncPath(data["directory"]).resolve()
            return cls.from_dict(
                data=data, serialization_adapter=serialization_adapter, key_sanitization_strategy=key_sanitization_strategy
            )

        msg = f"File {file} not found"

        raise FileNotFoundError(msg)

    @classmethod
    async def create_or_get_info(
        cls,
        *,
        data_directory: AsyncPath,
        metadata_directory: AsyncPath,
        collection: str,
        sanitized_collection: str,
        serialization_adapter: SerializationAdapter,
        key_sanitization_strategy: SanitizationStrategy,
    ) -> Self:
        info_file: AsyncPath = AsyncPath(metadata_directory / f"{sanitized_collection}-info.json")

        if await info_file.exists():
            return await cls.from_file(
                file=info_file, serialization_adapter=serialization_adapter, key_sanitization_strategy=key_sanitization_strategy
            )

        info = cls(
            collection=collection,
            directory=data_directory,
            created_at=now(),
            serialization_adapter=serialization_adapter,
            key_sanitization_strategy=key_sanitization_strategy,
        )

        await write_file(file=info_file, text=info.to_json())
        return info

    # async def cull_entry(self, *, key: str) -> bool:
    #     if entry := await self.get_entry(key=key):
    #         if entry.is_expired:
    #             await self.delete_entry(key=key)
    #             return True

    #     return False

    # async def cull_all_entries(self, *, serialization_adapter: SerializationAdapter) -> int:
    #     deleted_count: int = 0
    #     async for key in self.list_entries():
    #         if await self.cull_entry(key=key.stem, serialization_adapter=serialization_adapter):
    #             deleted_count += 1
    #     return deleted_count


async def read_file(file: AsyncPath) -> dict[str, Any]:
    async with aopen(file_specifier=Path(file), mode="r", encoding="utf-8") as f:
        body: str = await f.read()
        return load_from_json(json_str=body)


async def write_file(file: AsyncPath, text: str) -> None:
    async with aopen(file_specifier=Path(file), mode="w", encoding="utf-8") as f:
        await f.write(data=text)


class FileTreeStore(BaseStore):
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

    By default, collections and keys are not sanitized. This means that filesystem limitations
    on path lengths and special characters may cause errors when trying to get and put entries.

    To avoid issues, you may want to consider leveraging the `FileTreeV1CollectionSanitizationStrategy`
    and `FileTreeV1KeySanitizationStrategy` strategies.

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

    _data_directory: AsyncPath
    _metadata_directory: AsyncPath

    _collection_infos: dict[str, DiskCollectionInfo]

    def __init__(
        self,
        *,
        data_directory: Path | str,
        metadata_directory: Path | str | None = None,
        serialization_adapter: SerializationAdapter | None = None,
        key_sanitization_strategy: SanitizationStrategy | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the file-tree store.

        Args:
            data_directory: The base directory to use for storing collections and keys.
            metadata_directory: The directory to use for storing metadata. Defaults to data_directory.
            serialization_adapter: The serialization adapter to use for the store.
            key_sanitization_strategy: The sanitization strategy to use for keys.
            collection_sanitization_strategy: The sanitization strategy to use for collections.
            default_collection: The default collection to use if no collection is provided.
        """
        data_directory = Path(data_directory).resolve()
        data_directory.mkdir(parents=True, exist_ok=True)

        if metadata_directory is None:
            metadata_directory = data_directory

        metadata_directory = Path(metadata_directory).resolve()
        metadata_directory.mkdir(parents=True, exist_ok=True)

        self._data_directory = AsyncPath(data_directory)
        self._metadata_directory = AsyncPath(metadata_directory)

        self._collection_infos = {}

        self._stable_api = False

        super().__init__(
            serialization_adapter=serialization_adapter or BasicSerializationAdapter(),
            key_sanitization_strategy=key_sanitization_strategy,
            collection_sanitization_strategy=collection_sanitization_strategy,
            default_collection=default_collection,
        )

    async def _get_data_directories(self) -> AsyncGenerator[AsyncPath]:
        async for directory in self._data_directory.iterdir():
            if await directory.is_dir():
                yield directory

    async def _get_metadata_entries(self) -> AsyncGenerator[AsyncPath]:
        async for entry in self._metadata_directory.iterdir():
            if await entry.is_file() and entry.suffix == ".json":
                yield await entry.resolve()

    async def _load_collection_infos(self) -> None:
        async for entry in self._get_metadata_entries():
            collection_info: DiskCollectionInfo = await DiskCollectionInfo.from_file(
                file=entry,
                serialization_adapter=self._serialization_adapter,
                key_sanitization_strategy=self._key_sanitization_strategy,
            )
            self._collection_infos[collection_info.collection] = collection_info

    # async def _generate_collection_path(self, collection: str) -> AsyncPath:
    #     """Get the path to a collection directory.

    #     Args:
    #         collection: The collection name.

    #     Returns:
    #         The path to the collection directory.

    #     Raises:
    #         ValueError: If the collection name contains path separators or would escape the base directory.
    #     """

    #     # Reject path separators to prevent nested directories
    #     if "/" in collection or "\\" in collection:
    #         msg = f"Invalid collection name: {collection!r} cannot contain path separators"
    #         raise ValueError(msg)

    #     collection_path = await AsyncPath(self._directory / collection).resolve()

    #     if not collection_path.is_relative_to(self._directory):
    #         msg = f"Invalid collection name: {collection!r} would escape base directory"
    #         raise ValueError(msg)

    #     return collection_path

    @override
    async def _setup_collection(self, *, collection: str) -> None:
        """Set up a collection by creating its directory if it doesn't exist.

        Args:
            collection: The collection name.
        """
        if collection in self._collection_infos:
            return

        # Sanitize the collection name using the strategy
        sanitized_collection = self._sanitize_collection(collection=collection)

        # Create the collection directory under the data directory
        data_directory: AsyncPath = AsyncPath(self._data_directory / sanitized_collection)
        await data_directory.mkdir(parents=True, exist_ok=True)

        self._collection_infos[collection] = await DiskCollectionInfo.create_or_get_info(
            data_directory=data_directory,
            metadata_directory=self._metadata_directory,
            collection=collection,
            sanitized_collection=sanitized_collection,
            serialization_adapter=self._serialization_adapter,
            key_sanitization_strategy=self._key_sanitization_strategy,
        )

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        """Retrieve a managed entry by key from the specified collection.

        Args:
            collection: The collection name.
            key: The key name.

        Returns:
            The managed entry if found and not expired, None otherwise.
        """
        collection_info: DiskCollectionInfo = self._collection_infos[collection]

        return await collection_info.get_entry(key=key)

    @override
    async def _put_managed_entry(self, *, key: str, collection: str, managed_entry: ManagedEntry) -> None:
        """Store a managed entry at the specified key in the collection.

        Args:
            collection: The collection name.
            key: The key name.
            managed_entry: The managed entry to store.
        """
        collection_info: DiskCollectionInfo = self._collection_infos[collection]
        await collection_info.put_entry(key=key, data=managed_entry)

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        """Delete a managed entry from the specified collection.

        Args:
            collection: The collection name.
            key: The key name.

        Returns:
            True if the entry was deleted, False if it didn't exist.
        """
        collection_info: DiskCollectionInfo = self._collection_infos[collection]

        return await collection_info.delete_entry(key=key)

    # async def _gen_collection_keys(self, *, collection: str) -> AsyncGenerator[str]:
    #     """Generate all keys in the specified collection."""
    #     collection_info: DiskCollectionInfo = self._collection_infos[collection]
    #     async for item_path in collection_info.list_entries():
    #         yield item_path.stem

    # @override
    # async def _get_collection_keys(self, *, collection: str, limit: int | None = None) -> list[str]:
    #     """List all keys in the specified collection.

    #     Args:
    #         collection: The collection name.
    #         limit: Maximum number of keys to return.

    #     Returns:
    #         A list of key names (without the .json extension).
    #     """
    #     limit = min(limit or DEFAULT_PAGE_SIZE, PAGE_LIMIT)
    #     keys: list[str] = []
    #     async for key in self._gen_collection_keys(collection=collection):
    #         keys.append(key)
    #         if len(keys) >= limit:
    #             break
    #     return keys

    # @override
    # async def _get_collection_names(self, *, limit: int | None = None) -> list[str]:
    #     """List all collection names.

    #     Args:
    #         limit: Maximum number of collections to return.

    #     Returns:
    #         A list of collection names.
    #     """
    #     if limit is None:
    #         limit = DEFAULT_PAGE_SIZE

    #     collections: list[str] = [collection_info.collection for collection_info in self._collection_infos.values()]

    #     return collections[:limit]

    # @override
    # async def _delete_collection(self, *, collection: str) -> bool:
    #     """Delete an entire collection and all its keys.

    #     Args:
    #         collection: The collection name.

    #     Returns:
    #         True if the collection was deleted, False if it didn't exist or an error occurred.
    #     """
    #     collection_info: DiskCollectionInfo = self._collection_infos[collection]

    #     await collection_info.delete_all_entries()

    #     del self._collection_infos[collection]

    #     return True

    # @override
    # async def _delete_store(self) -> bool:
    #     """Delete the entire store and all its collections.

    #     Returns:
    #         True if the store was deleted successfully.
    #     """

    #     for collection_info in self._collection_infos.values():
    #         await self._delete_collection(collection=collection_info.collection)

    #     return True

    # async def _cull_collection(self, *, collection: str) -> int:
    #     """Cull a collection."""
    #     collection_info: DiskCollectionInfo = self._collection_infos[collection]
    #     deleted_count: int = 0
    #     async for file in collection_path.iterdir():
    #         if await self._cull_file(file=file):
    #             deleted_count += 1
    #     return deleted_count

    # async def _cull_directory(self, *, directory: AsyncPath) -> int:
    #     """Cull the directory."""
    #     deleted_count: int = 0
    #     async for collection_path in directory.iterdir():
    #         if await self._cull_file(file=collection_path):
    #             deleted_count += 1
    #     return deleted_count

    # async def _cull_key(self, *, key: str, collection: str) -> bool:
    #     """Cull a key."""
    #     key_path = self._get_key_path(collection=collection, key=key)
    #     return await self._cull_file(file=key_path)

    # async def _cull_file(self, *, file: AsyncPath) -> bool:
    #     """Cull a file. If it is a managed entry, check if it is expired and remove it if it is."""
    #     managed_entry: ManagedEntry | None = await self._read_file(file=file)
    #     if managed_entry is not None and not managed_entry.is_expired:
    #         return False

    #     await file.unlink()

    #     return True

    # async def _get_collection_info(self, *, directory: AsyncPath) -> DiskCollectionInfo:
    #     """Get the collection info."""
    #     info_file: AsyncPath = await (directory / "info.json").resolve()
    #     return await DiskCollectionInfo.from_file(file=info_file)

    # async def _read_file(self, *, file: AsyncPath) -> ManagedEntry | None:
    #     """Read a file and return the managed entry."""
    #     if not await file.is_file() or file.suffix != ".json":
    #         return None

    #     json_str = await file.read_text(encoding="utf-8")
    #     return self._serialization_adapter.load_json(json_str=json_str)

    # async def cleanup_expired(self, *, collection: str | None = None) -> int:
    #     """Remove expired entries from disk.

    #     This utility method scans the specified collection (or all collections if None)
    #     and deletes entries that have expired based on their TTL. This is useful for
    #     manual cleanup of the store since expired entries are not automatically removed.

    #     Args:
    #         collection: The collection to clean up, or None to clean all collections.

    #     Returns:
    #         The number of entries deleted.
    #     """

    #     if collection is not None:
    #         return await self._cull_collection(collection=collection)

    #     deleted_count: int = 0

    #     async for directory in self._get_collection_directories():
    #         deleted_count += await self._cull_directory(directory=directory)

    #     return deleted_count
