"""Windows Registry-based key-value store."""

from typing import Literal
from winreg import HKEY_CURRENT_USER, HKEY_LOCAL_MACHINE

from key_value.shared.utils.managed_entry import ManagedEntry
from key_value.shared.utils.sanitize import ALPHANUMERIC_CHARACTERS, sanitize_string
from typing_extensions import override

from key_value.aio.stores.base import BaseStore

try:
    import winreg
except ImportError as e:
    msg = "WindowsRegistryStore requires Windows platform (winreg module)"
    raise ImportError(msg) from e

MAX_KEY_LENGTH = 96
ALLOWED_KEY_CHARACTERS: str = ALPHANUMERIC_CHARACTERS

MAX_COLLECTION_LENGTH = 96
ALLOWED_COLLECTION_CHARACTERS: str = ALPHANUMERIC_CHARACTERS
DEFAULT_REGISTRY_PATH = "Software\\py-key-value"
DEFAULT_HIVE = "HKEY_CURRENT_USER"


class WindowsRegistryStore(BaseStore):
    """Windows Registry-based key-value store.

    This store uses the Windows Registry to persist key-value pairs. Each entry is stored
    as a string value in the registry under HKEY_CURRENT_USER\\Software\\{root}\\{collection}\\{key}.

    Note: TTL is not natively supported by Windows Registry, so TTL information is stored
    within the JSON payload and checked at retrieval time.
    """

    def __init__(
        self,
        *,
        hive: Literal["HKEY_CURRENT_USER", "HKEY_LOCAL_MACHINE"] | None = None,
        registry_path: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the Windows Registry store.

        Args:
            hive: The hive to use. Defaults to "HKEY_CURRENT_USER".
            registry_path: The registry path to use. Must be a valid registry path under the hive. Defaults to "Software\\py-key-value".
            default_collection: The default collection to use if no collection is provided.
        """
        self._hive = HKEY_LOCAL_MACHINE if hive == "HKEY_LOCAL_MACHINE" else HKEY_CURRENT_USER
        self._registry_path = registry_path or DEFAULT_REGISTRY_PATH

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

    def _get_registry_path(self, *, collection: str) -> str:
        """Get the full registry path for a collection."""
        sanitized_collection = self._sanitize_collection_name(collection=collection)
        return f"{self._registry_path}\\{sanitized_collection}"

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        sanitized_key = self._sanitize_key(key=key)
        registry_path = self._get_registry_path(collection=collection)

        try:
            with winreg.OpenKey(key=self._hive, sub_key=registry_path, reserved=0, access=winreg.KEY_READ) as reg_key:
                json_str, _ = winreg.QueryValueEx(reg_key, sanitized_key)
        except (FileNotFoundError, OSError):
            return None

        if json_str is None:
            return None

        return ManagedEntry.from_json(json_str=json_str)

    @override
    async def _put_managed_entry(self, *, key: str, collection: str, managed_entry: ManagedEntry) -> None:
        sanitized_key = self._sanitize_key(key=key)
        registry_path = self._get_registry_path(collection=collection)

        json_str: str = managed_entry.to_json()

        try:
            with winreg.CreateKey(self._hive, registry_path) as reg_key:
                winreg.SetValueEx(reg_key, sanitized_key, 0, winreg.REG_SZ, json_str)
        except OSError as e:
            msg = f"Failed to write to registry: {e}"
            raise RuntimeError(msg) from e

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        sanitized_key = self._sanitize_key(key=key)
        registry_path = self._get_registry_path(collection=collection)

        try:
            with winreg.OpenKey(self._hive, registry_path, 0, winreg.KEY_WRITE) as reg_key:
                winreg.DeleteValue(reg_key, sanitized_key)
        except (FileNotFoundError, OSError):
            return False
        else:
            return True
