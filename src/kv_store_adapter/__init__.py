"""
KV Store Adapter - A pluggable interface for Key-Value stores.

This package provides a common interface for various KV store implementations
including in-memory, disk-based, and Redis-based stores.
"""

from .protocol import KVStoreProtocol
from .exceptions import KVStoreError, KeyNotFoundError, TTLError

__version__ = "0.1.0"
__all__ = ["KVStoreProtocol", "KVStoreError", "KeyNotFoundError", "TTLError"]