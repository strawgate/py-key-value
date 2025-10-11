"""macOS Keychain-based key-value store."""

from contextlib import suppress
from typing import overload

from key_value.shared.utils.compound import compound_key, get_collections_from_compound_keys, get_keys_from_compound_keys
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import BaseDestroyStore, BaseEnumerateCollectionsStore, BaseEnumerateKeysStore, BaseStore

try:
    import keyring
    from keyring.errors import PasswordDeleteError
except ImportError as e:
    msg = "KeychainStore requires py-key-value-aio[keychain]"
    raise ImportError(msg) from e

DEFAULT_KEYCHAIN_SERVICE = "py-key-value"


class KeychainStore(BaseEnumerateCollectionsStore, BaseEnumerateKeysStore, BaseDestroyStore, BaseStore):
    """macOS Keychain-based key-value store using keyring library.

    This store uses the macOS Keychain to persist key-value pairs. Each entry is stored
    as a password in the keychain with the combination of collection and key as the username.

    Note: TTL is not natively supported by macOS Keychain, so TTL information is stored
    within the JSON payload and checked at retrieval time.
    """

    _service_name: str
    _key_index: set[str]  # Track all compound keys for enumeration

    @overload
    def __init__(self, *, service_name: str = DEFAULT_KEYCHAIN_SERVICE, default_collection: str | None = None) -> None: ...

    def __init__(
        self,
        *,
        service_name: str = DEFAULT_KEYCHAIN_SERVICE,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the macOS Keychain store.

        Args:
            service_name: The service name to use in the keychain. Defaults to "py-key-value".
            default_collection: The default collection to use if no collection is provided.
        """
        self._service_name = service_name
        self._key_index = set()

        super().__init__(default_collection=default_collection)

    @override
    async def _setup(self) -> None:
        """Initialize the store by loading existing keys."""
        # Note: keyring doesn't provide a way to enumerate all keys for a service,
        # so we maintain an index of keys in a special keychain entry
        try:
            index_data = keyring.get_password(self._service_name, "__index__")
            if index_data:
                self._key_index = set(index_data.split("\n"))
        except Exception:  # noqa: S110
            # If we can't load the index, start fresh
            pass

    def _save_index(self) -> None:
        """Save the key index to the keychain."""
        index_data = "\n".join(sorted(self._key_index))
        keyring.set_password(self._service_name, "__index__", index_data)

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        try:
            json_str: str | None = keyring.get_password(self._service_name, combo_key)
        except Exception:
            return None

        if json_str is None:
            return None

        return ManagedEntry.from_json(json_str=json_str)

    @override
    async def _put_managed_entry(self, *, key: str, collection: str, managed_entry: ManagedEntry) -> None:
        combo_key: str = compound_key(collection=collection, key=key)

        json_str: str = managed_entry.to_json()

        keyring.set_password(self._service_name, combo_key, json_str)

        # Update the index
        self._key_index.add(combo_key)
        self._save_index()

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)

        try:
            keyring.delete_password(self._service_name, combo_key)
        except PasswordDeleteError:
            return False
        else:
            self._key_index.discard(combo_key)
            self._save_index()
            return True

    @override
    async def _get_collection_keys(self, *, collection: str, limit: int | None = None) -> list[str]:
        return get_keys_from_compound_keys(compound_keys=list(self._key_index), collection=collection)

    @override
    async def _get_collection_names(self, *, limit: int | None = None) -> list[str]:
        return get_collections_from_compound_keys(compound_keys=list(self._key_index))

    @override
    async def _delete_store(self) -> bool:
        """Delete all entries from the store."""
        # Delete all indexed keys
        for combo_key in list(self._key_index):
            with suppress(PasswordDeleteError):
                keyring.delete_password(self._service_name, combo_key)

        # Clear the index
        self._key_index.clear()
        with suppress(PasswordDeleteError):
            keyring.delete_password(self._service_name, "__index__")

        return True
