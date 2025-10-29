"""Serialization adapters for converting ManagedEntry objects to/from store-specific formats.

This module provides a base SerializationAdapter ABC and common adapter implementations
that can be reused across different key-value stores.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from key_value.shared.errors.key_value import DeserializationError
from key_value.shared.utils.managed_entry import ManagedEntry, load_from_json, verify_dict
from key_value.shared.utils.time_to_live import try_parse_datetime_str


class SerializationAdapter(ABC):
    """Base class for store-specific serialization adapters.

    Adapters encapsulate the logic for converting between ManagedEntry objects
    and store-specific storage formats. This provides a consistent interface
    while allowing each store to optimize its serialization strategy.
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


class FullJsonAdapter(SerializationAdapter):
    """Adapter that serializes entries as complete JSON strings.

    This adapter is suitable for stores that work with string values,
    such as Redis or Valkey. It serializes the entire ManagedEntry
    (including all metadata) to a JSON string.
    """

    def to_storage(self, key: str, entry: ManagedEntry, collection: str | None = None) -> str:  # noqa: ARG002
        """Convert a ManagedEntry to a JSON string.

        Args:
            key: The key (unused, for interface compatibility).
            entry: The ManagedEntry to serialize.
            collection: The collection (unused, for interface compatibility).

        Returns:
            A JSON string containing the entry and all metadata.
        """
        return entry.to_json(include_metadata=True, include_expiration=True, include_creation=True)

    def from_storage(self, data: dict[str, Any] | str) -> ManagedEntry:
        """Convert a JSON string back to a ManagedEntry.

        Args:
            data: The JSON string to deserialize.

        Returns:
            A ManagedEntry reconstructed from the JSON.

        Raises:
            DeserializationError: If data is not a string or cannot be parsed.
        """
        if not isinstance(data, str):
            msg = "Expected data to be a JSON string"
            raise DeserializationError(msg)

        return ManagedEntry.from_json(json_str=data, includes_metadata=True)


class StringifiedDictAdapter(SerializationAdapter):
    """Adapter that serializes entries as dicts with stringified values.

    This adapter is suitable for stores that prefer to store entries as
    documents with the value field serialized as a JSON string. This allows
    stores to index and query metadata fields while keeping the value opaque.
    """

    def to_storage(self, key: str, entry: ManagedEntry, collection: str | None = None) -> dict[str, Any]:  # noqa: ARG002
        """Convert a ManagedEntry to a dict with stringified value.

        Args:
            key: The key associated with this entry.
            entry: The ManagedEntry to serialize.
            collection: The collection (unused, for interface compatibility).

        Returns:
            A dict with key, stringified value, and metadata fields.
        """
        return {
            "key": key,
            **entry.to_dict(include_metadata=True, include_expiration=True, include_creation=True, stringify_value=True),
        }

    def from_storage(self, data: dict[str, Any] | str) -> ManagedEntry:
        """Convert a dict with stringified value back to a ManagedEntry.

        Args:
            data: The dict to deserialize.

        Returns:
            A ManagedEntry reconstructed from the dict.

        Raises:
            DeserializationError: If data is not a dict or is malformed.
        """
        if not isinstance(data, dict):
            msg = "Expected data to be a dict"
            raise DeserializationError(msg)

        return ManagedEntry.from_dict(obj=data, expects_stringified_value=True, includes_metadata=True)


class MongoDBAdapter(SerializationAdapter):
    """MongoDB-specific serialization adapter with native BSON datetime support.

    This adapter is optimized for MongoDB, storing:
    - Native BSON datetime types for TTL indexing (created_at, expires_at)
    - Values in value.object (native BSON) or value.string (JSON) fields
    - Support for migration between native and string storage modes

    The native storage mode is recommended for new deployments as it allows
    efficient querying of value fields, while string mode provides backward
    compatibility with older data.
    """

    def __init__(self, *, native_storage: bool = True) -> None:
        """Initialize the MongoDB adapter.

        Args:
            native_storage: If True (default), store value as native BSON dict in value.object field.
                          If False, store as JSON string in value.string field for backward compatibility.
        """
        self.native_storage = native_storage

    def to_storage(self, key: str, entry: ManagedEntry, collection: str | None = None) -> dict[str, Any]:  # noqa: ARG002
        """Convert a ManagedEntry to a MongoDB document.

        Args:
            key: The key associated with this entry.
            entry: The ManagedEntry to serialize.
            collection: The collection (unused, for interface compatibility).

        Returns:
            A MongoDB document with key, value, and BSON datetime metadata.
        """
        document: dict[str, Any] = {"key": key, "value": {}}

        # We convert to JSON even if we don't need to, this ensures that the value we were provided
        # can be serialized to JSON which helps ensure compatibility across stores
        json_str = entry.value_as_json

        # Store in appropriate field based on mode
        if self.native_storage:
            document["value"]["object"] = entry.value_as_dict
        else:
            document["value"]["string"] = json_str

        # Add metadata fields as BSON datetimes for TTL indexing
        if entry.created_at:
            document["created_at"] = entry.created_at
        if entry.expires_at:
            document["expires_at"] = entry.expires_at

        return document

    def from_storage(self, data: dict[str, Any] | str) -> ManagedEntry:
        """Convert a MongoDB document back to a ManagedEntry.

        This method supports both native (object) and legacy (string) storage modes,
        and properly handles BSON datetime types for metadata.

        Args:
            data: The MongoDB document to deserialize.

        Returns:
            A ManagedEntry reconstructed from the document.

        Raises:
            DeserializationError: If data is not a dict or is malformed.
        """
        if not isinstance(data, dict):
            msg = "Expected MongoDB document to be a dict"
            raise DeserializationError(msg)

        document = data

        if not (value_field := document.get("value")):
            msg = "Value field not found"
            raise DeserializationError(msg)

        if not isinstance(value_field, dict):
            msg = "Expected `value` field to be an object"
            raise DeserializationError(msg)

        value_holder: dict[str, Any] = verify_dict(obj=value_field)

        entry_data: dict[str, Any] = {}

        # Mongo stores datetimes without timezones as UTC so we mark them as UTC
        # Import timezone here to avoid circular import
        from key_value.shared.utils.time_to_live import timezone

        if created_at_datetime := document.get("created_at"):
            if not isinstance(created_at_datetime, datetime):
                msg = "Expected `created_at` field to be a datetime"
                raise DeserializationError(msg)
            entry_data["created_at"] = created_at_datetime.replace(tzinfo=timezone.utc)

        if expires_at_datetime := document.get("expires_at"):
            if not isinstance(expires_at_datetime, datetime):
                msg = "Expected `expires_at` field to be a datetime"
                raise DeserializationError(msg)
            entry_data["expires_at"] = expires_at_datetime.replace(tzinfo=timezone.utc)

        # Support both native (object) and legacy (string) storage
        if value_object := value_holder.get("object"):
            return ManagedEntry.from_dict(data={"value": value_object, **entry_data})

        if value_string := value_holder.get("string"):
            return ManagedEntry.from_dict(data={"value": value_string, **entry_data}, stringified_value=True)

        msg = "Expected `value` field to be an object with `object` or `string` subfield"
        raise DeserializationError(msg)


class ElasticsearchAdapter(SerializationAdapter):
    """Adapter for Elasticsearch with support for native and string storage modes.

    This adapter supports two storage modes:
    - Native mode: Stores values as flattened dicts for efficient querying
    - String mode: Stores values as JSON strings for backward compatibility

    Elasticsearch-specific features:
    - Stores collection name in the document for multi-tenancy
    - Uses ISO format for datetime fields
    - Supports migration between storage modes
    """

    def __init__(self, *, native_storage: bool = True) -> None:
        """Initialize the Elasticsearch adapter.

        Args:
            native_storage: If True (default), store values as flattened dicts.
                          If False, store values as JSON strings.
        """
        self.native_storage = native_storage

    def to_storage(self, key: str, entry: ManagedEntry, collection: str | None = None) -> dict[str, Any]:
        """Convert a ManagedEntry to an Elasticsearch document.

        Args:
            key: The key associated with this entry.
            entry: The ManagedEntry to serialize.
            collection: The collection name to store in the document.

        Returns:
            An Elasticsearch document dict with collection, key, value, and metadata.
        """
        document: dict[str, Any] = {"collection": collection or "", "key": key, "value": {}}

        # Store in appropriate field based on mode
        if self.native_storage:
            document["value"]["flattened"] = entry.value_as_dict
        else:
            document["value"]["string"] = entry.value_as_json

        if entry.created_at:
            document["created_at"] = entry.created_at.isoformat()
        if entry.expires_at:
            document["expires_at"] = entry.expires_at.isoformat()

        return document

    def from_storage(self, data: dict[str, Any] | str) -> ManagedEntry:
        """Convert an Elasticsearch document back to a ManagedEntry.

        This method supports both native (flattened) and string storage modes,
        trying the flattened field first and falling back to the string field.
        This allows for seamless migration between storage modes.

        Args:
            data: The Elasticsearch document to deserialize.

        Returns:
            A ManagedEntry reconstructed from the document.

        Raises:
            DeserializationError: If data is not a dict or is malformed.
        """
        if not isinstance(data, dict):
            msg = "Expected Elasticsearch document to be a dict"
            raise DeserializationError(msg)

        document = data
        value: dict[str, Any] = {}

        raw_value = document.get("value")

        # Try flattened field first, fall back to string field
        if not raw_value or not isinstance(raw_value, dict):
            msg = "Value field not found or invalid type"
            raise DeserializationError(msg)

        if value_flattened := raw_value.get("flattened"):  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
            value = verify_dict(obj=value_flattened)
        elif value_str := raw_value.get("string"):  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
            if not isinstance(value_str, str):
                msg = "Value in `value` field is not a string"
                raise DeserializationError(msg)
            value = load_from_json(value_str)
        else:
            msg = "Value field not found or invalid type"
            raise DeserializationError(msg)

        created_at: datetime | None = try_parse_datetime_str(value=document.get("created_at"))
        expires_at: datetime | None = try_parse_datetime_str(value=document.get("expires_at"))

        return ManagedEntry(
            value=value,
            created_at=created_at,
            expires_at=expires_at,
        )
