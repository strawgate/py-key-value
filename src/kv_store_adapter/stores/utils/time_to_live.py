import time
from datetime import datetime, timedelta, timezone
from typing import Any


def epoch_to_datetime(epoch: float) -> datetime:
    """Convert an epoch timestamp to a datetime object."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def now_as_epoch() -> float:
    """Get the current time as epoch seconds."""
    return time.time()


def now() -> datetime:
    """Get the current time as a datetime object."""
    return datetime.now(tz=timezone.utc)


def seconds_to(datetime: datetime) -> float:
    """Get the number of seconds between the current time and a datetime object."""
    return (datetime - now()).total_seconds()


def now_plus(seconds: float) -> datetime:
    """Get the current time plus a number of seconds as a datetime object."""
    return datetime.now(tz=timezone.utc) + timedelta(seconds=seconds)


def try_parse_datetime(value: Any) -> datetime | None:  # pyright: ignore[reportAny]
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return None
