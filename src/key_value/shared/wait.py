"""Async wait utilities for testing and polling.

This module re-exports from key_value.aio._shared for backwards compatibility.
"""

from key_value.aio._shared.wait import async_wait_for_true

__all__ = ["async_wait_for_true"]
