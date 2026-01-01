import sys
from datetime import datetime, timezone
from typing import Any

import pytest

from key_value.shared.errors.key_value import InvalidTTLError
from key_value.shared.utils.time_to_live import prepare_entry_timestamps, prepare_ttl, try_parse_datetime_str

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


def test_prepare_ttl_zero():
    """Test that zero TTL raises InvalidTTLError."""
    with pytest.raises(InvalidTTLError):
        prepare_ttl(0)


def test_prepare_ttl_negative():
    """Test that negative TTL raises InvalidTTLError."""
    with pytest.raises(InvalidTTLError):
        prepare_ttl(-100)


class TestTryParseDatetimeStr:
    def test_valid_datetime_string(self):
        """Test parsing valid datetime string."""
        result = try_parse_datetime_str("2025-01-01T00:00:00+00:00")
        assert result == FIXED_DATETIME

    def test_invalid_datetime_string(self):
        """Test parsing invalid datetime string returns None."""
        result = try_parse_datetime_str("not-a-datetime")
        assert result is None

    def test_non_string_returns_none(self):
        """Test non-string values return None."""
        assert try_parse_datetime_str(12345) is None
        assert try_parse_datetime_str(None) is None
        assert try_parse_datetime_str({"key": "value"}) is None


class TestPrepareEntryTimestamps:
    def test_with_ttl(self):
        """Test prepare_entry_timestamps with a TTL."""
        created_at, ttl, expires_at = prepare_entry_timestamps(ttl=100)
        assert ttl == 100.0
        assert expires_at is not None
        assert expires_at > created_at

    def test_without_ttl(self):
        """Test prepare_entry_timestamps without a TTL."""
        created_at, ttl, expires_at = prepare_entry_timestamps(ttl=None)
        assert ttl is None
        assert expires_at is None
        assert created_at is not None
