"""Utilities for compounding and prefixing keys and collections.

This module re-exports from key_value.aio._shared for backwards compatibility.
"""

from key_value.aio._shared.compound import (
    DEFAULT_COMPOUND_SEPARATOR,
    DEFAULT_PREFIX_SEPARATOR,
    compound_key,
    compound_string,
    get_collections_from_compound_keys,
    get_keys_from_compound_keys,
    prefix_collection,
    prefix_key,
    uncompound_key,
    uncompound_string,
    uncompound_strings,
    unprefix_collection,
    unprefix_key,
)

__all__ = [
    "DEFAULT_COMPOUND_SEPARATOR",
    "DEFAULT_PREFIX_SEPARATOR",
    "compound_key",
    "compound_string",
    "get_collections_from_compound_keys",
    "get_keys_from_compound_keys",
    "prefix_collection",
    "prefix_key",
    "uncompound_key",
    "uncompound_string",
    "uncompound_strings",
    "unprefix_collection",
    "unprefix_key",
]
