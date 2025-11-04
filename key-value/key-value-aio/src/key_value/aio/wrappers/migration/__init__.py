"""Migration wrapper for gradual store transitions.

This module provides a wrapper that enables gradual migration between two stores
(e.g., stores with different configurations) without breaking access to existing data.
"""

from key_value.aio.wrappers.migration.wrapper import MigrationWrapper

__all__ = ["MigrationWrapper"]
