"""Base exception classes for key-value store errors.

This module re-exports from key_value.aio._shared for backwards compatibility.
"""

from key_value.aio._shared.errors.base import BaseKeyValueError, ExtraInfoType

__all__ = ["BaseKeyValueError", "ExtraInfoType"]
