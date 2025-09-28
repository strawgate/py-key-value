from key_value.aio.errors.base import BaseKeyValueError


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

    def __init__(self, ttl: float):
        super().__init__(
            message="A TTL is invalid.",
            extra_info={"ttl": ttl},
        )
