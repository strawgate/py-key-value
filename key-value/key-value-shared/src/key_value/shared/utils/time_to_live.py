import time
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, overload

from key_value.shared.errors import InvalidTTLError
from key_value.shared.errors.key_value import IncorrectTTLCountError


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


def try_parse_datetime_str(value: Any) -> datetime | None:  # pyright: ignore[reportAny]
    try:
        if isinstance(value, str):
            return datetime.fromisoformat(value)
    except ValueError:
        return None

    return None


@overload
def validate_ttl(t: float | int) -> float: ...


@overload
def validate_ttl(t: float | int | None) -> float | None: ...


def validate_ttl(t: float | int | None) -> float | None:
    if t is None:
        return None

    if not isinstance(t, float | int):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise InvalidTTLError(ttl=t)

    if isinstance(t, int):
        t = float(t)

    if t <= 0:
        raise InvalidTTLError(ttl=t)

    return t


def validate_ttls(t: Sequence[float | None] | float | None) -> list[float | None]:
    if not isinstance(t, Sequence):
        t = [t]
    return [validate_ttl(t=ttl) if ttl is not None else None for ttl in t]


def prepare_ttls(t: Sequence[float | None] | float | None, count: int) -> list[float | None]:
    if t is None:
        return [None] * count

    if isinstance(t, str):
        raise InvalidTTLError(ttl=t)

    if isinstance(t, (int, float)):
        t = [float(t)] * count

    if isinstance(t, Sequence):  # pyright: ignore[reportUnnecessaryIsInstance]
        if len(t) != count:
            raise IncorrectTTLCountError(ttl=t, count=count)

        t = [validate_ttl(t=ttl) for ttl in t]

    return t
