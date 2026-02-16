"""Wrapper-specific error classes for encryption, read-only, and size limiting.

This module re-exports from key_value.aio._shared for backwards compatibility.
"""

from key_value.aio._shared.errors.wrappers import (
    CorruptedDataError,
    DecryptionError,
    EncryptionError,
    EncryptionVersionError,
    EntryTooLargeError,
    EntryTooSmallError,
    ReadOnlyError,
)

__all__ = [
    "CorruptedDataError",
    "DecryptionError",
    "EncryptionError",
    "EncryptionVersionError",
    "EntryTooLargeError",
    "EntryTooSmallError",
    "ReadOnlyError",
]
