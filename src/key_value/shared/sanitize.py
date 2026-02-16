"""Low-level string sanitization functions.

This module re-exports from key_value.aio._shared for backwards compatibility.
"""

from key_value.aio._shared.sanitize import (
    ALPHANUMERIC_CHARACTERS,
    DEFAULT_HASH_FRAGMENT_SEPARATOR,
    DEFAULT_HASH_FRAGMENT_SIZE,
    DEFAULT_REPLACEMENT_CHARACTER,
    LOWERCASE_ALPHABET,
    MINIMUM_MAX_LENGTH,
    NUMBERS,
    UPPERCASE_ALPHABET,
    HashFragmentMode,
    generate_hash_fragment,
    hash_excess_length,
    sanitize_characters_in_string,
    sanitize_string,
)

__all__ = [
    "ALPHANUMERIC_CHARACTERS",
    "DEFAULT_HASH_FRAGMENT_SEPARATOR",
    "DEFAULT_HASH_FRAGMENT_SIZE",
    "DEFAULT_REPLACEMENT_CHARACTER",
    "LOWERCASE_ALPHABET",
    "MINIMUM_MAX_LENGTH",
    "NUMBERS",
    "UPPERCASE_ALPHABET",
    "HashFragmentMode",
    "generate_hash_fragment",
    "hash_excess_length",
    "sanitize_characters_in_string",
    "sanitize_string",
]
