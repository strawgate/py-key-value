"""
Base abstract class for unmanaged key-value store implementations.
"""

from abc import ABC, abstractmethod
from typing import Any

from kv_store_adapter.types import TTLInfo


class BaseKVStore(ABC):
    """Abstract base class for key-value store implementations.

    The "value" passed to the implementation will be a dictionary of the value to store.

    When using this ABC, your implementation will:
    1. Implement `get` and `set` to get and save values
    2. Self-manage Expiration
    3. Self-manage Collections
    4. Self-manage Expired Entry Culling
    """

    @abstractmethod
    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        """Retrieve a non-expired value by key from the specified collection."""
        ...

    @abstractmethod
    async def put(
        self,
        collection: str,
        key: str,
        value: dict[str, Any],
        *,
        ttl: float | None = None,
    ) -> None:
        """Store a key-value pair in the specified collection with optional TTL."""
        ...

    @abstractmethod
    async def delete(self, collection: str, key: str) -> bool:
        """Delete a key from the specified collection, returning True if it existed."""
        ...

    @abstractmethod
    async def ttl(self, collection: str, key: str) -> TTLInfo | None:
        """Get TTL information for a key, or None if the key doesn't exist."""
        ...

    @abstractmethod
    async def exists(self, collection: str, key: str) -> bool:
        """Check if a key exists in the specified collection."""

        return await self.get(collection=collection, key=key) is not None

    @abstractmethod
    async def keys(self, collection: str) -> list[str]:
        """List all keys in the specified collection."""

        ...

    @abstractmethod
    async def clear_collection(self, collection: str) -> int:
        """Clear all keys in a collection, returning the number of keys deleted."""
        ...

    @abstractmethod
    async def list_collections(self) -> list[str]:
        """List all available collection names (may include empty collections)."""
        ...

    @abstractmethod
    async def cull(self) -> None:
        """Remove all expired entries from the store."""
        ...
