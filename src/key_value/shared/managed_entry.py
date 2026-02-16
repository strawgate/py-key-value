"""ManagedEntry dataclass for storing values with metadata.

This module re-exports from key_value.aio._shared for backwards compatibility.
"""

# Re-export datetime for backwards compatibility (used by disk/multi_store.py)
from datetime import datetime

from key_value.aio._shared.managed_entry import (
    ManagedEntry,
    dump_to_json,
    estimate_serialized_size,
    load_from_json,
    verify_dict,
)

__all__ = [
    "ManagedEntry",
    "datetime",
    "dump_to_json",
    "estimate_serialized_size",
    "load_from_json",
    "verify_dict",
]
