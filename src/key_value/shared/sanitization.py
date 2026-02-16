"""Sanitization strategies for key and collection names.

This module re-exports from key_value.aio._shared for backwards compatibility.
"""

from key_value.aio._shared.sanitization import (
    MAXIMUM_HASH_LENGTH,
    MINIMUM_HASH_LENGTH,
    AlwaysHashStrategy,
    HashExcessLengthStrategy,
    HashFragmentMode,
    HybridSanitizationStrategy,
    PassthroughStrategy,
    SanitizationStrategy,
)

__all__ = [
    "MAXIMUM_HASH_LENGTH",
    "MINIMUM_HASH_LENGTH",
    "AlwaysHashStrategy",
    "HashExcessLengthStrategy",
    "HashFragmentMode",
    "HybridSanitizationStrategy",
    "PassthroughStrategy",
    "SanitizationStrategy",
]
