"""Store-level error classes.

This module re-exports from key_value.aio._shared for backwards compatibility.
"""

from key_value.aio._shared.errors.store import (
    KeyValueStoreError,
    PathSecurityError,
    StoreConnectionError,
    StoreSetupError,
)

__all__ = [
    "KeyValueStoreError",
    "PathSecurityError",
    "StoreConnectionError",
    "StoreSetupError",
]
