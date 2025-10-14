import sys
from datetime import datetime, timezone
from typing import Any

import pytest
from inline_snapshot import snapshot

from key_value.shared.errors.key_value import InvalidTTLError
from key_value.shared.utils.time_to_live import prepare_ttl, prepare_ttls

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
    assert prepare_ttls([t], 1) == [expected]
    assert prepare_ttls(t, 2) == [expected, expected]


def test_prepare_ttls_edge_cases():
    assert prepare_ttls(None, 1) == [None]
    assert prepare_ttls(None, 2) == [None, None]
    assert prepare_ttls([100, 100.0], 2) == [100.0, 100.0]


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
def test_prepare_ttl_invalid(t: Any):
    with pytest.raises(InvalidTTLError):
        prepare_ttl(t)

    with pytest.raises(InvalidTTLError):
        prepare_ttls([t], 1)

    with pytest.raises(InvalidTTLError):
        prepare_ttls(t, 2)


def test_prepare_ttls_string():
    with pytest.raises(InvalidTTLError) as exc_info:
        prepare_ttls("100", 1)  # type: ignore

    assert exc_info.value.extra_info == snapshot({"ttl": "100", "type": "str"})
