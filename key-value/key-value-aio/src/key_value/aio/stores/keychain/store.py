"""macOS Keychain-based key-value store."""

from typing import overload

from key_value.shared.utils.compound import compound_key
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import BaseStore

try:
    import keyring
    from keyring.errors import PasswordDeleteError
except ImportError as e:
    msg = "KeychainStore requires py-key-value-aio[keychain]"
    raise ImportError(msg) from e

DEFAULT_KEYCHAIN_SERVICE = "py-key-value"


class KeychainStore(BaseStore):
    """macOS Keychain-based key-value store using keyring library.

    This store uses the macOS Keychain to persist key-value pairs. Each entry is stored
    as a password in the keychain with the combination of collection and key as the username.

    Note: TTL is not natively supported by macOS Keychain, so TTL information is stored
    within the JSON payload and checked at retrieval time.

    Note: This store does not support enumeration of keys or collections as the keyring
    library does not provide these capabilities.
    """

    _service_name: str

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

        super().__init__(default_collection=default_collection)

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

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)

        try:
            keyring.delete_password(self._service_name, combo_key)
        except PasswordDeleteError:
            return False
        else:
            return True
