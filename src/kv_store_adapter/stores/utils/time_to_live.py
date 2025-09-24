from datetime import datetime, timedelta, timezone


def calculate_expires_at(created_at: datetime | None = None, ttl: float | None = None) -> datetime | None:
    """Calculate expiration timestamp from creation time and TTL seconds."""
    if ttl is None:
        return None

    expires_at: datetime = (created_at or datetime.now(tz=timezone.utc)) + timedelta(seconds=ttl)
    return expires_at
