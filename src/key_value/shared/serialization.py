"""Serialization adapter base class for converting ManagedEntry objects to/from store-specific formats.

This module re-exports from key_value.aio._shared for backwards compatibility.
"""

from key_value.aio._shared.serialization import (
    BasicSerializationAdapter,
    SerializationAdapter,
    key_must_be,
    parse_datetime_str,
)

__all__ = [
    "BasicSerializationAdapter",
    "SerializationAdapter",
    "key_must_be",
    "parse_datetime_str",
]
