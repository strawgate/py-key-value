"""Constants used across key-value store implementations.

This module re-exports from key_value.aio._shared for backwards compatibility.
"""

from key_value.aio._shared.constants import DEFAULT_COLLECTION_NAME

__all__ = ["DEFAULT_COLLECTION_NAME"]
