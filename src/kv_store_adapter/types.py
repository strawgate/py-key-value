from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable


@dataclass
class TTLInfo:
    """TTL (Time To Live) information for a key-value pair in a collection."""

    collection: str
    key: str
    created_at: datetime | None

    ttl: float | None
    expires_at: datetime | None

    @property
    def is_expired(self) -> bool:
        """Check if the key-value pair has expired based on its TTL."""
        if self.expires_at is None:
            return False

        return self.expires_at <= datetime.now(tz=timezone.utc)


@runtime_checkable
class KVStore(Protocol):
    """Protocol defining the interface for key-value store implementations."""

    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        """Retrieve a value by key from the specified collection."""
        ...

    async def put(self, collection: str, key: str, value: dict[str, Any], *, ttl: float | None = None) -> None:
        """Store a key-value pair in the specified collection with optional TTL."""
        ...

    async def delete(self, collection: str, key: str) -> bool:
        """Delete a key-value pair from the specified collection."""
        ...

    async def exists(self, collection: str, key: str) -> bool:
        """Check if a key exists in the specified collection."""
        ...


@runtime_checkable
class BulkKVStore(KVStore, Protocol):
    """Protocol defining the interface for bulk key-value store implementations."""

    async def get_many(self, collection: str, keys: list[str]) -> list[dict[str, Any]]:
        """Retrieve multiple values by key from the specified collection."""
        ...

    async def put_many(self, collection: str, keys: list[str], values: list[dict[str, Any]]) -> None:
        """Store multiple key-value pairs in the specified collection."""
        ...

    async def delete_many(self, collection: str, keys: list[str]) -> None:
        """Delete multiple key-value pairs from the specified collection."""
        ...


@runtime_checkable
class ManageKVStore(KVStore, Protocol):
    """Protocol defining the interface for managed key-value store implementations."""

    async def keys(self, collection: str) -> list[str]:
        """List all keys in the specified collection."""
        ...

    async def collections(self) -> list[str]:
        """List all available collection names (may include empty collections)."""
        ...

    async def delete_collection(self, collection: str) -> int:
        """Clear all keys in a collection, returning the number of keys deleted."""
        ...

    async def cull(self) -> None:
        """Remove all expired entries from the store."""
        ...
