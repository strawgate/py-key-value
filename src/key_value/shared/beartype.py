"""Beartype configuration and decorators for runtime type checking.

This module re-exports from key_value.aio._shared for backwards compatibility.
"""

from key_value.aio._shared.beartype import (
    bear_enforce,
    bear_spray,
    enforce_bear_type,
    enforce_bear_type_conf,
    no_bear_type,
    no_bear_type_check,
    no_bear_type_check_conf,
)

__all__ = [
    "bear_enforce",
    "bear_spray",
    "enforce_bear_type",
    "enforce_bear_type_conf",
    "no_bear_type",
    "no_bear_type_check",
    "no_bear_type_check_conf",
]
