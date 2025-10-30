import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, SupportsFloat

from typing_extensions import Self

from key_value.shared.errors import DeserializationError, SerializationError
from key_value.shared.utils.time_to_live import now, now_plus, seconds_to, try_parse_datetime_str


@dataclass(kw_only=True)
class ManagedEntry:
    """A managed cache entry containing value data and TTL metadata.

    The entry supports either TTL seconds or absolute expiration datetime. On init:
    - If `ttl` is provided but `expires_at` is not, an `expires_at` will be computed.
    - If `expires_at` is provided but `ttl` is not, a live TTL will be computed on access.
    """

    value: Mapping[str, Any]

    created_at: datetime | None = field(default=None)
    expires_at: datetime | None = field(default=None)

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return self.expires_at <= now()

    @property
    def ttl(self) -> float | None:
        if self.expires_at is None:
            return None
        return seconds_to(datetime=self.expires_at)

    @property
    def value_as_json(self) -> str:
        """Return the value as a JSON string."""
        return dump_to_json(obj=self.value_as_dict)

    @property
    def value_as_dict(self) -> dict[str, Any]:
        return verify_dict(obj=self.value)

    @property
    def created_at_isoformat(self) -> str | None:
        return self.created_at.isoformat() if self.created_at else None

    @property
    def expires_at_isoformat(self) -> str | None:
        return self.expires_at.isoformat() if self.expires_at else None

    @classmethod
    def from_ttl(cls, *, value: Mapping[str, Any], created_at: datetime | None = None, ttl: SupportsFloat) -> Self:
        return cls(
            value=value,
            created_at=created_at,
            expires_at=(now_plus(seconds=float(ttl)) if ttl else None),
        )

    def to_dict(
        self, include_metadata: bool = True, include_expiration: bool = True, include_creation: bool = True, stringify_value: bool = False
    ) -> dict[str, Any]:
        if not include_metadata:
            return dict(self.value)

        data: dict[str, Any] = {"value": self.value_as_json if stringify_value else self.value}

        if include_creation and self.created_at:
            data["created_at"] = self.created_at.isoformat()
        if include_expiration and self.expires_at:
            data["expires_at"] = self.expires_at.isoformat()

        return data

    def to_json(
        self, include_metadata: bool = True, include_expiration: bool = True, include_creation: bool = True, stringify_value: bool = False
    ) -> str:
        return dump_to_json(
            obj=self.to_dict(
                include_metadata=include_metadata,
                include_expiration=include_expiration,
                include_creation=include_creation,
                stringify_value=stringify_value,
            )
        )

    @classmethod
    def from_dict(  # noqa: PLR0912
        cls,
        data: dict[str, Any],
        includes_metadata: bool = True,
        stringified_value: bool = False,
    ) -> Self:
        if not includes_metadata:
            return cls(
                value=data,
            )

        created_at: datetime | None = None
        expires_at: datetime | None = None

        if created_at_value := data.get("created_at"):
            if isinstance(created_at_value, str):
                created_at = try_parse_datetime_str(value=created_at_value)
            elif isinstance(created_at_value, datetime):
                created_at = created_at_value
            else:
                msg = "Expected `created_at` field to be a string or datetime"
                raise DeserializationError(msg)

        if expires_at_value := data.get("expires_at"):
            if isinstance(expires_at_value, str):
                expires_at = try_parse_datetime_str(value=expires_at_value)
            elif isinstance(expires_at_value, datetime):
                expires_at = expires_at_value
            else:
                msg = "Expected `expires_at` field to be a string or datetime"
                raise DeserializationError(msg)

        if not (raw_value := data.get("value")):
            msg = "Value is None"
            raise DeserializationError(msg)

        value: dict[str, Any]

        if stringified_value:
            if not isinstance(raw_value, str):
                msg = "Value is not a string"
                raise DeserializationError(msg)
            value = load_from_json(json_str=raw_value)
        else:
            if not isinstance(raw_value, dict):
                msg = "Value is not a dictionary"
                raise DeserializationError(msg)
            value = verify_dict(obj=raw_value)

        return cls(
            created_at=created_at,
            expires_at=expires_at,
            value=value,
        )

    @classmethod
    def from_json(cls, json_str: str, includes_metadata: bool = True) -> Self:
        data: dict[str, Any] = load_from_json(json_str=json_str)

        return cls.from_dict(data=data, includes_metadata=includes_metadata)


def dump_to_json(obj: dict[str, Any]) -> str:
    try:
        return json.dumps(obj, sort_keys=True)
    except (json.JSONDecodeError, TypeError) as e:
        msg: str = f"Failed to serialize object to JSON: {e}"
        raise SerializationError(msg) from e


def load_from_json(json_str: str) -> dict[str, Any]:
    try:
        return verify_dict(obj=json.loads(json_str))  # pyright: ignore[reportAny]

    except (json.JSONDecodeError, TypeError) as e:
        msg: str = f"Failed to deserialize JSON string: {e}"
        raise DeserializationError(msg) from e


def verify_dict(obj: Any) -> dict[str, Any]:
    if not isinstance(obj, Mapping):
        msg = "Object is not a dictionary"
        raise DeserializationError(msg)

    if not all(isinstance(key, str) for key in obj):  # pyright: ignore[reportUnknownVariableType]
        msg = "Object contains non-string keys"
        raise DeserializationError(msg)

    return dict(obj)  # pyright: ignore[reportUnknownArgumentType]


def estimate_serialized_size(value: Mapping[str, Any]) -> int:
    """Estimate the serialized size of a value without creating a ManagedEntry.

    This function provides a more efficient way to estimate the size of a value
    when serialized to JSON, without the overhead of creating a full ManagedEntry object.
    This is useful for size-based checks in wrappers.

    Args:
        value: The value mapping to estimate the size for.

    Returns:
        The estimated size in bytes when serialized to JSON.
    """
    return len(dump_to_json(obj=dict(value)))
