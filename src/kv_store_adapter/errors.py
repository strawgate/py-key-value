from typing import Any

ExtraInfoType = dict[str, Any]


class KVStoreAdapterError(Exception):
    """Base exception for all KV Store Adapter errors."""

    def __init__(self, message: str | None = None, extra_info: ExtraInfoType | None = None):
        message_parts: list[str] = []

        if message:
            message_parts.append(message)

        if extra_info:
            extra_info_str = ";".join(f"{k}: {v}" for k, v in extra_info.items())  # pyright: ignore[reportAny]
            if message:
                extra_info_str = "(" + extra_info_str + ")"

            message_parts.append(extra_info_str)

        super().__init__(": ".join(message_parts))


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
