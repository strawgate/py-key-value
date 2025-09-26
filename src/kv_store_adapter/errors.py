from typing import Any

ExtraInfoType = dict[str, str | int | float | bool | None]


class KVStoreAdapterError(Exception):
    """Base exception for all KV Store Adapter errors."""

    def __init__(self, message: str | None = None, extra_info: ExtraInfoType | None = None):
        message_parts: list[str] = []

        if message:
            message_parts.append(message)

        if extra_info:
            extra_info_str = ";".join(f"{k}: {v}" for k, v in extra_info.items())
            if message:
                extra_info_str = "(" + extra_info_str + ")"

            message_parts.append(extra_info_str)

        super().__init__(": ".join(message_parts))


class MissingKeyError(KVStoreAdapterError):
    """Raised when a key is missing from the store."""

    def __init__(self, operation: str, collection: str | None = None, key: str | None = None):
        super().__init__(
            message="A key was requested that was required but not found in the store.",
            extra_info={"operation": operation, "collection": collection or "default", "key": key},
        )


class InvalidTTLError(KVStoreAdapterError):
    """Raised when a TTL is invalid."""

    def __init__(self, ttl: float):
        super().__init__(
            message="A TTL is invalid.",
            extra_info={"ttl": ttl},
        )


class SetupError(KVStoreAdapterError):
    """Raised when a store setup fails."""


class UnknownError(KVStoreAdapterError):
    """Raised when an unexpected or unidentifiable error occurs."""


class StoreConnectionError(KVStoreAdapterError):
    """Raised when unable to connect to or communicate with the underlying store."""


class KVStoreAdapterOperationError(KVStoreAdapterError):
    """Raised when a store operation fails due to operational issues."""


class SerializationError(KVStoreAdapterOperationError):
    """Raised when data cannot be serialized for storage."""


class DeserializationError(KVStoreAdapterOperationError):
    """Raised when stored data cannot be deserialized back to its original form."""


class ConfigurationError(KVStoreAdapterError):
    """Raised when store configuration is invalid or incomplete."""
