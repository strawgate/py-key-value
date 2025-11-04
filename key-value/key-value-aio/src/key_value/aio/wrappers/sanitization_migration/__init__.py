"""Sanitization migration wrapper for gradual strategy transitions.

This module provides a wrapper that enables gradual migration between different
sanitization strategies without breaking access to existing data.
"""

from key_value.aio.wrappers.sanitization_migration.wrapper import SanitizationMigrationWrapper

__all__ = ["SanitizationMigrationWrapper"]
