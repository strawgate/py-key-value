import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, SupportsFloat, cast

from typing_extensions import Self

from key_value.shared.errors import DeserializationError, SerializationError
from key_value.shared.utils.time_to_live import now, now_plus, try_parse_datetime_str


@dataclass(kw_only=True)
class ManagedEntry:
    """A managed cache entry containing value data and TTL metadata.

    The entry supports either TTL seconds or absolute expiration datetime. On init:
    - If `ttl` is provided but `expires_at` is not, an `expires_at` will be computed.
    - If `expires_at` is provided but `ttl` is not, a live TTL will be computed on access.
    """

    value: dict[str, Any]

    created_at: datetime | None = field(default=None)
    ttl: float | None = field(default=None)
    expires_at: datetime | None = field(default=None)

    _dumped_json: str | None = field(default=None)

    def __post_init__(self) -> None:
        if self.ttl is not None and self.expires_at is None:
            self.expires_at = now_plus(seconds=self.ttl)

        elif self.expires_at is not None and self.ttl is None:
            self.recalculate_ttl()

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return self.expires_at <= now()

    def recalculate_ttl(self) -> None:
        if self.expires_at is not None and self.ttl is None:
            self.ttl = (self.expires_at - now()).total_seconds()

    def to_json(self, include_metadata: bool = True, include_expiration: bool = True, include_creation: bool = True) -> str:
        data: dict[str, Any] = {}

        if include_metadata:
            data["value"] = self.value
            if include_creation and self.created_at:
                data["created_at"] = self.created_at.isoformat()
            if include_expiration and self.expires_at:
                data["expires_at"] = self.expires_at.isoformat()
        else:
            data = self.value

        return dump_to_json(obj=data)

    @classmethod
    def from_json(cls, json_str: str, includes_metadata: bool = True, ttl: SupportsFloat | None = None) -> Self:
        data: dict[str, Any] = load_from_json(json_str=json_str)

        if not includes_metadata:
            return cls(
                value=data,
            )

        created_at: datetime | None = try_parse_datetime_str(value=data.get("created_at"))
        expires_at: datetime | None = try_parse_datetime_str(value=data.get("expires_at"))

        value: dict[str, Any] | None = data.get("value")

        if value is None:
            msg = "Value is None"
            raise DeserializationError(msg)

        return cls(
            created_at=created_at,
            expires_at=expires_at,
            ttl=float(ttl) if ttl else None,
            value=value,
        )


def dump_to_json(obj: dict[str, Any]) -> str:
    try:
        return json.dumps(obj)
    except (json.JSONDecodeError, TypeError) as e:
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
