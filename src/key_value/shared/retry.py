"""Async retry utilities with exponential backoff.

This module re-exports from key_value.aio._shared for backwards compatibility.
"""

from key_value.aio._shared.retry import async_retry_operation

__all__ = ["async_retry_operation"]
