import time
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from numbers import Real
from typing import Any, SupportsFloat, overload

from key_value.shared.errors import InvalidTTLError


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
def prepare_ttl(t: SupportsFloat) -> float: ...


@overload
def prepare_ttl(t: SupportsFloat | None) -> float | None: ...


def prepare_ttl(t: SupportsFloat | None) -> float | None:
    """Prepare a TTL for use in a put operation.

    If a TTL is provided, it will be validated and returned as a float.
    If a None is provided, None will be returned.

    If the provided TTL is not a float or float-adjacent type, an InvalidTTLError will be raised. In addition,
    if a bool is provided, an InvalidTTLError will be raised. If the user passes TTL=True, true becomes `1` and the
    entry immediately expires which is likely not what the user intended.
    """
    if t is None:
        return None

    # This is not needed by the static type checker but is needed by the runtime type checker
    if not isinstance(t, Real | SupportsFloat) or isinstance(t, bool):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise InvalidTTLError(ttl=t, extra_info={"type": type(t).__name__})

    ttl = float(t)

    if ttl <= 0:
        raise InvalidTTLError(ttl=t)

    return ttl


