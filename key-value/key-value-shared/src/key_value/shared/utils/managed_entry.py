import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, SupportsFloat, cast

from typing_extensions import Self

from key_value.shared.errors import DeserializationError, SerializationError
from key_value.shared.utils.time_to_live import now, now_plus, prepare_ttl, try_parse_datetime_str


@dataclass(kw_only=True)
class ManagedEntry:
    """A managed cache entry containing value data and TTL metadata.

    The entry supports either TTL seconds or absolute expiration datetime. On init:
    - If `ttl` is provided but `expires_at` is not, an `expires_at` will be computed.
    - If `expires_at` is provided but `ttl` is not, a live TTL will be computed on access.
    """

    value: Mapping[str, Any]

    created_at: datetime | None = field(default=None)
    ttl: float | None = field(default=None)
    expires_at: datetime | None = field(default=None)

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

    @property
    def value_as_json(self) -> str:
        """Return the value as a JSON string."""
        return dump_to_json(obj=self.value_as_dict)

    @property
    def value_as_dict(self) -> dict[str, Any]:
        return dict(self.value)

    def recalculate_ttl(self) -> None:
        if self.expires_at is not None and self.ttl is None:
            self.ttl = (self.expires_at - now()).total_seconds()

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
    def from_dict(
        cls, data: dict[str, Any], includes_metadata: bool = True, ttl: SupportsFloat | None = None, stringified_value: bool = False
    ) -> Self:
        if not includes_metadata:
            return cls(
                value=data,
            )

        created_at: datetime | None = try_parse_datetime_str(value=data.get("created_at"))
        expires_at: datetime | None = try_parse_datetime_str(value=data.get("expires_at"))

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

        ttl_seconds: float | None = prepare_ttl(t=ttl)

        return cls(
            created_at=created_at,
            expires_at=expires_at,
            ttl=ttl_seconds,
            value=value,
        )

    @classmethod
    def from_json(cls, json_str: str, includes_metadata: bool = True, ttl: SupportsFloat | None = None) -> Self:
        data: dict[str, Any] = load_from_json(json_str=json_str)

        return cls.from_dict(data=data, includes_metadata=includes_metadata, ttl=ttl)


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
    if not isinstance(obj, dict):
        msg = "Object is not a dictionary"
        raise DeserializationError(msg)

    if not all(isinstance(key, str) for key in obj):  # pyright: ignore[reportUnknownVariableType]
        msg = "Object contains non-string keys"
        raise DeserializationError(msg)

    return cast(typ="dict[str, Any]", val=obj)
