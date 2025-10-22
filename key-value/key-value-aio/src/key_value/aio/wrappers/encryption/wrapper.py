import base64
import json
from collections.abc import Sequence
from typing import Any, SupportsFloat

from cryptography.fernet import Fernet
from key_value.shared.errors.key_value import SerializationError
from typing_extensions import override

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.wrappers.base import BaseWrapper

# Special keys used to store encrypted data
_ENCRYPTED_DATA_KEY = "__encrypted_data__"
_ENCRYPTION_VERSION_KEY = "__encryption_version__"
_ENCRYPTION_VERSION = 1


class EncryptionWrapper(BaseWrapper):
    """Wrapper that encrypts values before storing and decrypts on retrieval.

    This wrapper encrypts the JSON-serialized value using Fernet (symmetric encryption)
    and stores it as a base64-encoded string within a special key in the dictionary.
    This allows encryption while maintaining the dict[str, Any] interface.

    The encrypted format looks like:
    {
        "__encrypted_data__": "base64-encoded-encrypted-data",
        "__encryption_version__": 1
    }

    Note: The encryption key must be kept secret and secure. If the key is lost,
    encrypted data cannot be recovered.
    """

    def __init__(
        self,
        key_value: AsyncKeyValue,
        encryption_key: bytes | str,
    ) -> None:
        """Initialize the encryption wrapper.

        Args:
            key_value: The store to wrap.
            encryption_key: The encryption key to use. Can be a bytes object or a base64-encoded string.
                          Use Fernet.generate_key() to generate a new key.
        """
        self.key_value: AsyncKeyValue = key_value

        # Convert string key to bytes if needed
        if isinstance(encryption_key, str):
            encryption_key = encryption_key.encode("utf-8")

        self._fernet = Fernet(encryption_key)

        super().__init__()

    def _encrypt_value(self, value: dict[str, Any]) -> dict[str, Any]:
        """Encrypt a value into the encrypted format."""
        # Don't encrypt if it's already encrypted
        if _ENCRYPTED_DATA_KEY in value:
            return value

        # Serialize to JSON
        try:
            json_str = json.dumps(value, separators=(",", ":"))
        except (json.JSONDecodeError, TypeError) as e:
            msg: str = f"Failed to serialize object to JSON: {e}"
            raise SerializationError(msg) from e

        json_bytes = json_str.encode("utf-8")

        # Encrypt with Fernet
        encrypted_bytes = self._fernet.encrypt(json_bytes)

        # Encode to base64 for storage in dict (though Fernet output is already base64)
        base64_str = base64.b64encode(encrypted_bytes).decode("ascii")

        return {
            _ENCRYPTED_DATA_KEY: base64_str,
            _ENCRYPTION_VERSION_KEY: _ENCRYPTION_VERSION,
        }

    def _decrypt_value(self, value: dict[str, Any] | None) -> dict[str, Any] | None:
        """Decrypt a value from the encrypted format."""
        if value is None:
            return None

        # Check if it's encrypted
        if _ENCRYPTED_DATA_KEY not in value:
            return value

        # Extract encrypted data
        base64_str = value[_ENCRYPTED_DATA_KEY]
        if not isinstance(base64_str, str):
            # Corrupted data, return as-is
            return value

        try:
            # Decode from base64
            encrypted_bytes = base64.b64decode(base64_str)

            # Decrypt with Fernet
            json_bytes = self._fernet.decrypt(encrypted_bytes)

            # Parse JSON
            json_str = json_bytes.decode("utf-8")
            return json.loads(json_str)  # type: ignore[no-any-return]
        except Exception:
            # If decryption fails, return the original value
            # This handles cases where data might be corrupted or encrypted with a different key
            return value

    @override
    async def get(self, key: str, *, collection: str | None = None) -> dict[str, Any] | None:
        value = await self.key_value.get(key=key, collection=collection)
        return self._decrypt_value(value)

    @override
    async def get_many(self, keys: list[str], *, collection: str | None = None) -> list[dict[str, Any] | None]:
        values = await self.key_value.get_many(keys=keys, collection=collection)
        return [self._decrypt_value(value) for value in values]

    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[dict[str, Any] | None, float | None]:
        value, ttl = await self.key_value.ttl(key=key, collection=collection)
        return self._decrypt_value(value), ttl

    @override
    async def ttl_many(self, keys: list[str], *, collection: str | None = None) -> list[tuple[dict[str, Any] | None, float | None]]:
        results = await self.key_value.ttl_many(keys=keys, collection=collection)
        return [(self._decrypt_value(value), ttl) for value, ttl in results]

    @override
    async def put(self, key: str, value: dict[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        encrypted_value = self._encrypt_value(value)
        return await self.key_value.put(key=key, value=encrypted_value, collection=collection, ttl=ttl)

    @override
    async def put_many(
        self,
        keys: list[str],
        values: Sequence[dict[str, Any]],
        *,
        collection: str | None = None,
        ttl: Sequence[SupportsFloat | None] | None = None,
    ) -> None:
        encrypted_values = [self._encrypt_value(value) for value in values]
        return await self.key_value.put_many(keys=keys, values=encrypted_values, collection=collection, ttl=ttl)
