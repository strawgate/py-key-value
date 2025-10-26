from typing import Any

from key_value.shared.utils.managed_entry import ManagedEntry


def managed_entry_to_document(key: str, managed_entry: ManagedEntry) -> dict[str, Any]:
    """
    Convert a ManagedEntry to a MongoDB document.
    """
    document: dict[str, Any] = {
        "key": key,
        "value": managed_entry.to_json(include_metadata=False),
    }

    if managed_entry.created_at:
        document["created_at"] = managed_entry.created_at.isoformat()
    if managed_entry.expires_at:
        document["expires_at"] = managed_entry.expires_at.isoformat()

    return document
