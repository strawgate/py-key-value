"""Python keyring-based key-value store."""

from key_value.shared.utils.compound import compound_key
from key_value.shared.utils.managed_entry import ManagedEntry
from key_value.shared.utils.sanitize import ALPHANUMERIC_CHARACTERS, sanitize_string
from typing_extensions import override

from key_value.aio.stores.base import BaseStore

try:
    import keyring
    from keyring.errors import PasswordDeleteError
except ImportError as e:
    msg = "KeyringStore requires py-key-value-aio[keyring]"
    raise ImportError(msg) from e

DEFAULT_KEYCHAIN_SERVICE = "py-key-value"
MAX_KEY_LENGTH = 256
ALLOWED_KEY_CHARACTERS: str = ALPHANUMERIC_CHARACTERS

MAX_COLLECTION_LENGTH = 256
ALLOWED_COLLECTION_CHARACTERS: str = ALPHANUMERIC_CHARACTERS


class KeyringStore(BaseStore):
    """Python keyring-based key-value store using keyring library.

    This store uses the Python keyring to persist key-value pairs. Each entry is stored
    as a password in the keychain with the combination of collection and key as the username.

    Note: TTL is not natively supported by Python keyring, so TTL information is stored
    within the JSON payload and checked at retrieval time.
    """

    _service_name: str

    def __init__(
        self,
        *,
        service_name: str = DEFAULT_KEYCHAIN_SERVICE,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the Python keyring store.

        Args:
            service_name: The service name to use in the keychain. Defaults to "py-key-value".
            default_collection: The default collection to use if no collection is provided.
        """
        self._service_name = service_name

        super().__init__(default_collection=default_collection)

    def _sanitize_collection_name(self, collection: str) -> str:
        return sanitize_string(
            value=collection,
            max_length=MAX_COLLECTION_LENGTH,
            allowed_characters=ALLOWED_COLLECTION_CHARACTERS,
        )

    def _sanitize_key(self, key: str) -> str:
        return sanitize_string(
            value=key,
            max_length=MAX_KEY_LENGTH,
            allowed_characters=ALLOWED_KEY_CHARACTERS,
        )

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        sanitized_collection = self._sanitize_collection_name(collection=collection)
        sanitized_key = self._sanitize_key(key=key)

        combo_key: str = compound_key(collection=sanitized_collection, key=sanitized_key)

        try:
            json_str: str | None = keyring.get_password(service_name=self._service_name, username=combo_key)
        except Exception:
            return None

        if json_str is None:
            return None

        return ManagedEntry.from_json(json_str=json_str)

    @override
    async def _put_managed_entry(self, *, key: str, collection: str, managed_entry: ManagedEntry) -> None:
        sanitized_collection = self._sanitize_collection_name(collection=collection)
        sanitized_key = self._sanitize_key(key=key)

        combo_key: str = compound_key(collection=sanitized_collection, key=sanitized_key)

        json_str: str = managed_entry.to_json()

        keyring.set_password(service_name=self._service_name, username=combo_key, password=json_str)

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        sanitized_collection = self._sanitize_collection_name(collection=collection)
        sanitized_key = self._sanitize_key(key=key)

        combo_key: str = compound_key(collection=sanitized_collection, key=sanitized_key)

        try:
            keyring.delete_password(service_name=self._service_name, username=combo_key)
        except PasswordDeleteError:
            return False
        else:
            return True
