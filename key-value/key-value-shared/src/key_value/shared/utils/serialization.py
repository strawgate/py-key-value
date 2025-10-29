"""Serialization adapter base class for converting ManagedEntry objects to/from store-specific formats.

This module provides the SerializationAdapter ABC that store implementations should use
to define their own serialization strategy. Store-specific adapter implementations
should be defined within their respective store modules.
"""

from abc import ABC, abstractmethod
from typing import Any

from key_value.shared.utils.managed_entry import ManagedEntry


class SerializationAdapter(ABC):
    """Base class for store-specific serialization adapters.

    Adapters encapsulate the logic for converting between ManagedEntry objects
    and store-specific storage formats. This provides a consistent interface
    while allowing each store to optimize its serialization strategy.

    Store implementations should subclass this adapter and define their own
    to_storage() and from_storage() methods within their store module.
    """

    @abstractmethod
    def to_storage(self, key: str, entry: ManagedEntry, collection: str | None = None) -> dict[str, Any] | str:
        """Convert a ManagedEntry to the store's storage format.

        Args:
            key: The key associated with this entry.
            entry: The ManagedEntry to serialize.
            collection: Optional collection name.

        Returns:
            The serialized representation (dict or str depending on store).
        """
        ...

    @abstractmethod
    def from_storage(self, data: dict[str, Any] | str) -> ManagedEntry:
        """Convert stored data back to a ManagedEntry.

        Args:
            data: The stored representation to deserialize.

        Returns:
            A ManagedEntry reconstructed from storage.

        Raises:
            DeserializationError: If the data cannot be deserialized.
        """
        ...
