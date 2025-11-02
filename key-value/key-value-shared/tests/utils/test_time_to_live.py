import sys
from datetime import datetime, timezone
from typing import Any

import pytest

from key_value.shared.errors.key_value import InvalidTTLError
from key_value.shared.utils.time_to_live import prepare_ttl

FIXED_DATETIME = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("t", "expected"),
    [
        (100, 100),
        (None, None),
        (100.0, 100.0),
        (0.1, 0.1),
        (sys.maxsize, float(sys.maxsize)),
    ],
    ids=["int", "none", "float", "float-0.1", "int-maxsize"],
)
def test_prepare_ttl(t: Any, expected: int | float | None):
    assert prepare_ttl(t) == expected


@pytest.mark.parametrize(
    ("t"),
    [
        "100",
        "None",
        "-100",
        "-None",
        "0.1",
        FIXED_DATETIME,
        FIXED_DATETIME.isoformat(),
        object(),
        {},
        True,
        False,
    ],
    ids=[
        "string",
        "string-none",
        "string-negative-int",
        "string-negative-none",
        "string-float",
        "datetime",
        "datetime-isoformat",
        "object",
        "dict",
        "bool-true",
        "bool-false",
    ],
)
@pytest.mark.filterwarnings("ignore:Function key_value.shared.utils")  # Ignore BearType warnings here
def test_prepare_ttl_invalid(t: Any):
    with pytest.raises(InvalidTTLError):
        prepare_ttl(t)
