from typing import Any

from key_value.shared.errors.base import BaseKeyValueError, ExtraInfoType


class KeyValueOperationError(BaseKeyValueError):
    """Base exception for all Key-Value operation errors."""


class SerializationError(KeyValueOperationError):
    """Raised when data cannot be serialized for storage."""


class DeserializationError(KeyValueOperationError):
    """Raised when stored data cannot be deserialized back to its original form."""


class MissingKeyError(KeyValueOperationError):
    """Raised when a key is missing from the store."""

    def __init__(self, operation: str, collection: str | None = None, key: str | None = None):
        super().__init__(
            message="A key was requested that was required but not found in the store.",
            extra_info={"operation": operation, "collection": collection or "default", "key": key},
        )


class InvalidTTLError(KeyValueOperationError):
    """Raised when a TTL is invalid."""

    def __init__(self, ttl: Any, extra_info: ExtraInfoType | None = None):
        super().__init__(
            message="A TTL is invalid.",
            extra_info={"ttl": str(ttl), **(extra_info or {})},
        )


class IncorrectTTLCountError(KeyValueOperationError):
    """Raised when the number of TTLs is incorrect."""

    def __init__(self, ttl: Any, count: int):
        super().__init__(
            message="The number of TTLs is incorrect.",
            extra_info={"ttl": ttl, "count": count},
        )
