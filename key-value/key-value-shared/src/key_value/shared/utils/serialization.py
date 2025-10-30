"""Serialization adapter base class for converting ManagedEntry objects to/from store-specific formats.

This module provides the SerializationAdapter ABC that store implementations should use
to define their own serialization strategy. Store-specific adapter implementations
should be defined within their respective store modules.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Literal, TypeVar

from key_value.shared.errors import DeserializationError
from key_value.shared.utils.managed_entry import ManagedEntry, dump_to_json, load_from_json, verify_dict
from key_value.shared.utils.time_to_live import try_parse_datetime_str

T = TypeVar("T")


def key_must_be(dictionary: dict[str, Any], /, key: str, expected_type: type[T]) -> T | None:
    if key not in dictionary:
        return None
    if not isinstance(dictionary[key], expected_type):
        msg = f"{key} must be a {expected_type.__name__}"
        raise TypeError(msg)
    return dictionary[key]


class SerializationAdapter(ABC):
    """Base class for store-specific serialization adapters.

    Adapters encapsulate the logic for converting between ManagedEntry objects
    and store-specific storage formats. This provides a consistent interface
    while allowing each store to optimize its serialization strategy.
    """

    _date_format: Literal["isoformat", "datetime"] | None = "isoformat"
    _value_format: Literal["string", "dict"] | None = "dict"

    def __init__(
        self, *, date_format: Literal["isoformat", "datetime"] | None = "isoformat", value_format: Literal["string", "dict"] | None = "dict"
    ) -> None:
        self._date_format = date_format
        self._value_format = value_format

    def load_json(self, json_str: str) -> ManagedEntry:
        """Convert a JSON string to a dictionary."""
        loaded_data: dict[str, Any] = load_from_json(json_str=json_str)

        return self.load_dict(data=loaded_data)

    @abstractmethod
    def prepare_load(self, data: dict[str, Any]) -> dict[str, Any]:
        """Prepare data for loading.

        This method is used by subclasses to handle any required transformations before loading the data into a ManagedEntry."""

    def load_dict(self, data: dict[str, Any]) -> ManagedEntry:
        """Convert a dictionary to a ManagedEntry."""

        data = self.prepare_load(data=data)

        managed_entry_proto: dict[str, Any] = {}

        if self._date_format == "isoformat":
            if created_at := key_must_be(data, key="created_at", expected_type=str):
                managed_entry_proto["created_at"] = try_parse_datetime_str(value=created_at)
            if expires_at := key_must_be(data, key="expires_at", expected_type=str):
                managed_entry_proto["expires_at"] = try_parse_datetime_str(value=expires_at)

        if self._date_format == "datetime":
            if created_at := key_must_be(data, key="created_at", expected_type=datetime):
                managed_entry_proto["created_at"] = created_at
            if expires_at := key_must_be(data, key="expires_at", expected_type=datetime):
                managed_entry_proto["expires_at"] = expires_at

        if not (value := data.get("value")):
            msg = "Value field not found"
            raise DeserializationError(message=msg)

        managed_entry_value: dict[str, Any] = {}

        if isinstance(value, str):
            managed_entry_value = load_from_json(json_str=value)
        elif isinstance(value, dict):
            managed_entry_value = verify_dict(obj=value)
        else:
            msg = "Value field is not a string or dictionary"
            raise DeserializationError(message=msg)

        return ManagedEntry(
            value=managed_entry_value,
            created_at=managed_entry_proto.get("created_at"),
            expires_at=managed_entry_proto.get("expires_at"),
        )

    @abstractmethod
    def prepare_dump(self, data: dict[str, Any]) -> dict[str, Any]:
        """Prepare data for dumping to a dictionary.

        This method is used by subclasses to handle any required transformations before dumping the data to a dictionary."""

    def dump_dict(self, entry: ManagedEntry, exclude_none: bool = True) -> dict[str, Any]:
        """Convert a ManagedEntry to a dictionary."""

        data: dict[str, Any] = {
            "value": entry.value_as_dict if self._value_format == "dict" else entry.value_as_json,
            "created_at": entry.created_at_isoformat,
            "expires_at": entry.expires_at_isoformat,
        }

        if exclude_none:
            data = {k: v for k, v in data.items() if v is not None}

        return self.prepare_dump(data=data)

    def dump_json(self, entry: ManagedEntry, exclude_none: bool = True) -> str:
        """Convert a ManagedEntry to a JSON string."""
        return dump_to_json(obj=self.dump_dict(entry=entry, exclude_none=exclude_none))


class BasicSerializationAdapter(SerializationAdapter):
    """Basic serialization adapter that does not perform any transformations."""

    def __init__(
        self, *, date_format: Literal["isoformat", "datetime"] | None = "isoformat", value_format: Literal["string", "dict"] | None = "dict"
    ) -> None:
        super().__init__(date_format=date_format, value_format=value_format)

    def prepare_load(self, data: dict[str, Any]) -> dict[str, Any]:
        return data

    def prepare_dump(self, data: dict[str, Any]) -> dict[str, Any]:
        return data


class ValueOnlySerializationAdapter(SerializationAdapter):
    """Serialization adapter that only serializes the value."""

    def __init__(self, *, value_format: Literal["string", "dict"] | None = "dict") -> None:
        super().__init__(date_format=None, value_format=value_format)

    def prepare_load(self, data: dict[str, Any]) -> dict[str, Any]:
        return {
            "value": data,
        }

    def prepare_dump(self, data: dict[str, Any]) -> dict[str, Any]:
        if "value" not in data:
            msg = "Value field not found"
            raise DeserializationError(message=msg)
        return verify_dict(obj=data["value"])
