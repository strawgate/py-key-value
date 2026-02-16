"""TTL handling and datetime utilities.

This module re-exports from key_value.aio._shared for backwards compatibility.
"""

from key_value.aio._shared.time_to_live import (
    epoch_to_datetime,
    now,
    now_as_epoch,
    now_plus,
    prepare_entry_timestamps,
    prepare_ttl,
    seconds_to,
    try_parse_datetime_str,
)

__all__ = [
    "epoch_to_datetime",
    "now",
    "now_as_epoch",
    "now_plus",
    "prepare_entry_timestamps",
    "prepare_ttl",
    "seconds_to",
    "try_parse_datetime_str",
]
