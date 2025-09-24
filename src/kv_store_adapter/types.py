from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol


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


class KVStoreProtocol(Protocol):
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
