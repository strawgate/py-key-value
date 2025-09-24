import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

from typing_extensions import Self

from kv_store_adapter.errors import DeserializationError, SerializationError
from kv_store_adapter.types import TTLInfo


@dataclass
class ManagedEntry:
    """A managed cache entry containing value data and TTL metadata."""

    collection: str
    key: str

    value: dict[str, Any]

    created_at: datetime | None
    ttl: float | None
    expires_at: datetime | None

    @property
    def is_expired(self) -> bool:
        return self.to_ttl_info().is_expired

    def to_ttl_info(self) -> TTLInfo:
        return TTLInfo(collection=self.collection, key=self.key, created_at=self.created_at, ttl=self.ttl, expires_at=self.expires_at)

    def to_json(self) -> str:
        return dump_to_json(
            obj={
                "created_at": self.created_at.isoformat() if self.created_at else None,
                "ttl": self.ttl,
                "expires_at": self.expires_at.isoformat() if self.expires_at else None,
                "collection": self.collection,
                "key": self.key,
                "value": self.value,
            }
        )

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        data: dict[str, Any] = load_from_json(json_str=json_str)
        created_at: str | None = data.get("created_at")
        expires_at: str | None = data.get("expires_at")
        ttl: float | None = data.get("ttl")

        return cls(
            created_at=datetime.fromisoformat(created_at) if created_at else None,
            ttl=ttl,
            expires_at=datetime.fromisoformat(expires_at) if expires_at else None,
            collection=data["collection"],  # pyright: ignore[reportAny]
            key=data["key"],  # pyright: ignore[reportAny]
            value=data["value"],  # pyright: ignore[reportAny]
        )


def dump_to_json(obj: dict[str, Any]) -> str:
    try:
        return json.dumps(obj)
    except json.JSONDecodeError as e:
        msg: str = f"Failed to serialize object to JSON: {e}"
        raise SerializationError(msg) from e


def load_from_json(json_str: str) -> dict[str, Any]:
    try:
        deserialized_obj: Any = json.loads(json_str)  # pyright: ignore[reportAny]

    except (json.JSONDecodeError, TypeError) as e:
        msg: str = f"Failed to deserialize JSON string: {e}"
        raise DeserializationError(msg) from e

    if not isinstance(deserialized_obj, dict):
        msg = "Deserialized object is not a dictionary"
        raise DeserializationError(msg)

    if not all(isinstance(key, str) for key in deserialized_obj):  # pyright: ignore[reportUnknownVariableType]
        msg = "Deserialized object contains non-string keys"
        raise DeserializationError(msg)

    return cast(typ="dict[str, Any]", val=deserialized_obj)
