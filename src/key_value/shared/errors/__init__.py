"""Error classes for key-value store operations.

This module re-exports from key_value.aio._shared for backwards compatibility.

Exception Hierarchy:
    BaseKeyValueError (base for all KV errors)
    ├── KeyValueOperationError (operation-level errors)
    │   ├── SerializationError
    │   ├── DeserializationError
    │   ├── MissingKeyError
    │   ├── InvalidTTLError
    │   ├── InvalidKeyError
    │   ├── ValueTooLargeError
    │   ├── EncryptionError
    │   │   ├── DecryptionError
    │   │   │   └── CorruptedDataError
    │   │   └── EncryptionVersionError
    │   ├── ReadOnlyError
    │   ├── EntryTooLargeError
    │   └── EntryTooSmallError
    └── KeyValueStoreError (store-level errors)
        ├── StoreSetupError
        └── StoreConnectionError
"""

from key_value.aio._shared.errors import (
    BaseKeyValueError,
    CorruptedDataError,
    DecryptionError,
    DeserializationError,
    EncryptionError,
    EncryptionVersionError,
    EntryTooLargeError,
    EntryTooSmallError,
    ExtraInfoType,
    InvalidKeyError,
    InvalidTTLError,
    KeyValueOperationError,
    KeyValueStoreError,
    MissingKeyError,
    PathSecurityError,
    ReadOnlyError,
    SerializationError,
    StoreConnectionError,
    StoreSetupError,
    ValueTooLargeError,
)

__all__ = [
    "BaseKeyValueError",
    "CorruptedDataError",
    "DecryptionError",
    "DeserializationError",
    "EncryptionError",
    "EncryptionVersionError",
    "EntryTooLargeError",
    "EntryTooSmallError",
    "ExtraInfoType",
    "InvalidKeyError",
    "InvalidTTLError",
    "KeyValueOperationError",
    "KeyValueStoreError",
    "MissingKeyError",
    "PathSecurityError",
    "ReadOnlyError",
    "SerializationError",
    "StoreConnectionError",
    "StoreSetupError",
    "ValueTooLargeError",
]
