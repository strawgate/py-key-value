"""Key-value operation error classes.

This module re-exports from key_value.aio._shared for backwards compatibility.
"""

from key_value.aio._shared.errors.key_value import (
    DeserializationError,
    InvalidKeyError,
    InvalidTTLError,
    KeyValueOperationError,
    MissingKeyError,
    SerializationError,
    ValueTooLargeError,
)

__all__ = [
    "DeserializationError",
    "InvalidKeyError",
    "InvalidTTLError",
    "KeyValueOperationError",
    "MissingKeyError",
    "SerializationError",
    "ValueTooLargeError",
]
