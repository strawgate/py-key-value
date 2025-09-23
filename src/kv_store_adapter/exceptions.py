"""
Custom exceptions for KV Store operations.
"""


class KVStoreError(Exception):
    """Base exception for all KV store operations."""
    pass


class KeyNotFoundError(KVStoreError):
    """Raised when a key is not found in the store."""
    pass


class TTLError(KVStoreError):
    """Raised when there's an error with TTL operations."""
    pass


class NamespaceError(KVStoreError):
    """Raised when there's an error with namespace operations."""
    pass