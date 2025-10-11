import contextlib

from key_value.shared.utils.compound import compound_key, get_collections_from_compound_keys, get_keys_from_compound_keys
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import BaseDestroyStore, BaseEnumerateCollectionsStore, BaseEnumerateKeysStore, BaseStore

try:
    import keyring  # pyright: ignore[reportMissingImports]
except ImportError as e:
    msg = "WindowsStore requires py-key-value-aio[windows]"
    raise ImportError(msg) from e

DEFAULT_SERVICE_NAME = "py-key-value"


class WindowsStore(BaseEnumerateCollectionsStore, BaseEnumerateKeysStore, BaseDestroyStore, BaseStore):
    """A Windows Credential Manager store that uses the keyring library to store data."""

    _service_name: str
    _stored_keys: set[str]

    def __init__(self, *, service_name: str = DEFAULT_SERVICE_NAME, default_collection: str | None = None) -> None:
        """Initialize the Windows Credential Manager store.

        Args:
            service_name: The service name to use for storing credentials. Defaults to "py-key-value".
            default_collection: The default collection to use if no collection is provided.
        """
        self._service_name = service_name
        self._stored_keys = set()

        super().__init__(default_collection=default_collection)

    async def _setup(self) -> None:
        """Initialize the store by loading existing keys."""
        # Note: keyring doesn't provide a way to list all credentials
        # So we maintain our own index
        index_key = f"{self._service_name}:__index__"
        index_data: str | None = keyring.get_password(self._service_name, index_key)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

        if index_data:
            # Parse the stored keys from the index
            self._stored_keys = set(index_data.split("\n"))  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]

    def _save_index(self) -> None:
        """Save the index of stored keys."""
        index_key = f"{self._service_name}:__index__"
        index_data = "\n".join(sorted(self._stored_keys))
        keyring.set_password(self._service_name, index_key, index_data)  # pyright: ignore[reportUnknownMemberType]

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        managed_entry_str: str | None = keyring.get_password(self._service_name, combo_key)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

        if not managed_entry_str:
            return None

        managed_entry: ManagedEntry = ManagedEntry.from_json(json_str=managed_entry_str)  # pyright: ignore[reportUnknownArgumentType]

        return managed_entry

    @override
    async def _put_managed_entry(self, *, key: str, collection: str, managed_entry: ManagedEntry) -> None:
        combo_key: str = compound_key(collection=collection, key=key)

        keyring.set_password(self._service_name, combo_key, managed_entry.to_json())  # pyright: ignore[reportUnknownMemberType]

        # Update the index
        self._stored_keys.add(combo_key)
        self._save_index()

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)

        try:
            keyring.delete_password(self._service_name, combo_key)  # pyright: ignore[reportUnknownMemberType]
        except keyring.errors.PasswordDeleteError:  # pyright: ignore[reportUnknownMemberType]
            return False
        else:
            # Update the index
            self._stored_keys.discard(combo_key)
            self._save_index()
            return True

    @override
    async def _get_collection_keys(self, *, collection: str, limit: int | None = None) -> list[str]:
        return get_keys_from_compound_keys(compound_keys=list(self._stored_keys), collection=collection)

    @override
    async def _get_collection_names(self, *, limit: int | None = None) -> list[str]:
        return get_collections_from_compound_keys(compound_keys=list(self._stored_keys))

    @override
    async def _delete_store(self) -> bool:
        # Delete all credentials associated with this service
        # Build list first to avoid modifying during iteration
        keys_to_delete = list(self._stored_keys)
        for combo_key in keys_to_delete:
            with contextlib.suppress(keyring.errors.PasswordDeleteError):  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
                keyring.delete_password(self._service_name, combo_key)  # pyright: ignore[reportUnknownMemberType]

        # Clear the index
        self._stored_keys.clear()
        index_key = f"{self._service_name}:__index__"
        with contextlib.suppress(keyring.errors.PasswordDeleteError):  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            keyring.delete_password(self._service_name, index_key)  # pyright: ignore[reportUnknownMemberType]

        return True
