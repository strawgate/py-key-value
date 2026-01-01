from datetime import datetime, timedelta, timezone

import pytest
from inline_snapshot import snapshot

from key_value.shared.errors import DeserializationError, SerializationError
from key_value.shared.utils.managed_entry import ManagedEntry
from key_value.shared.utils.serialization import BasicSerializationAdapter, key_must_be, parse_datetime_str

FIXED_DATETIME_ONE = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
FIXED_DATETIME_ONE_ISOFORMAT = FIXED_DATETIME_ONE.isoformat()
FIXED_DATETIME_ONE_PLUS_10_SECONDS = FIXED_DATETIME_ONE + timedelta(seconds=10)
FIXED_DATETIME_ONE_PLUS_10_SECONDS_ISOFORMAT = FIXED_DATETIME_ONE_PLUS_10_SECONDS.isoformat()

FIXED_DATETIME_TWO = datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
FIXED_DATETIME_TWO_PLUS_10_SECONDS = FIXED_DATETIME_TWO + timedelta(seconds=10)
FIXED_DATETIME_TWO_ISOFORMAT = FIXED_DATETIME_TWO.isoformat()
FIXED_DATETIME_TWO_PLUS_10_SECONDS_ISOFORMAT = FIXED_DATETIME_TWO_PLUS_10_SECONDS.isoformat()

TEST_DATA_ONE = {"key_one": "value_one", "key_two": "value_two", "key_three": {"nested_key": "nested_value"}}
TEST_ENTRY_ONE = ManagedEntry(value=TEST_DATA_ONE, created_at=FIXED_DATETIME_ONE, expires_at=FIXED_DATETIME_ONE_PLUS_10_SECONDS)
TEST_DATA_TWO = {"key_one": ["value_one", "value_two", "value_three"], "key_two": 123, "key_three": {"nested_key": "nested_value"}}
TEST_ENTRY_TWO = ManagedEntry(value=TEST_DATA_TWO, created_at=FIXED_DATETIME_TWO, expires_at=FIXED_DATETIME_TWO_PLUS_10_SECONDS)


@pytest.fixture
def serialization_adapter() -> BasicSerializationAdapter:
    return BasicSerializationAdapter()


class TestBasicSerializationAdapter:
    @pytest.fixture
    def adapter(self) -> BasicSerializationAdapter:
        return BasicSerializationAdapter()

    def test_empty_dict(self, adapter: BasicSerializationAdapter):
        managed_entry = adapter.load_json(
            json_str='{"created_at": "2025-01-01T00:00:00+00:00", "expires_at": "2025-01-01T00:00:10+00:00", "value": {}}'
        )
        assert managed_entry == snapshot(
            ManagedEntry(value={}, created_at=FIXED_DATETIME_ONE, expires_at=FIXED_DATETIME_ONE_PLUS_10_SECONDS)
        )

        managed_entry = adapter.load_dict(
            data={"created_at": FIXED_DATETIME_ONE_ISOFORMAT, "expires_at": FIXED_DATETIME_ONE_PLUS_10_SECONDS_ISOFORMAT, "value": {}}
        )
        assert managed_entry == snapshot(
            ManagedEntry(value={}, created_at=FIXED_DATETIME_ONE, expires_at=FIXED_DATETIME_ONE_PLUS_10_SECONDS)
        )

    def test_entry_one(self, adapter: BasicSerializationAdapter):
        assert adapter.dump_dict(entry=TEST_ENTRY_ONE) == snapshot(
            {
                "version": 1,
                "value": TEST_DATA_ONE,
                "created_at": FIXED_DATETIME_ONE_ISOFORMAT,
                "expires_at": FIXED_DATETIME_ONE_PLUS_10_SECONDS_ISOFORMAT,
            }
        )

        assert adapter.dump_json(entry=TEST_ENTRY_ONE) == snapshot(
            '{"created_at": "2025-01-01T00:00:00+00:00", "expires_at": "2025-01-01T00:00:10+00:00", "value": {"key_one": "value_one", "key_three": {"nested_key": "nested_value"}, "key_two": "value_two"}, "version": 1}'
        )

        assert adapter.load_dict(data=adapter.dump_dict(entry=TEST_ENTRY_ONE)) == snapshot(TEST_ENTRY_ONE)
        assert adapter.load_json(json_str=adapter.dump_json(entry=TEST_ENTRY_ONE)) == snapshot(TEST_ENTRY_ONE)

    def test_entry_two(self, adapter: BasicSerializationAdapter):
        assert adapter.dump_dict(entry=TEST_ENTRY_TWO) == snapshot(
            {
                "version": 1,
                "value": TEST_DATA_TWO,
                "created_at": FIXED_DATETIME_TWO_ISOFORMAT,
                "expires_at": FIXED_DATETIME_TWO_PLUS_10_SECONDS_ISOFORMAT,
            }
        )

        assert adapter.dump_json(entry=TEST_ENTRY_TWO) == snapshot(
            '{"created_at": "2025-01-01T00:00:01+00:00", "expires_at": "2025-01-01T00:00:11+00:00", "value": {"key_one": ["value_one", "value_two", "value_three"], "key_three": {"nested_key": "nested_value"}, "key_two": 123}, "version": 1}'
        )

        assert adapter.load_dict(data=adapter.dump_dict(entry=TEST_ENTRY_TWO)) == snapshot(TEST_ENTRY_TWO)
        assert adapter.load_json(json_str=adapter.dump_json(entry=TEST_ENTRY_TWO)) == snapshot(TEST_ENTRY_TWO)

    def test_dump_dict_with_key_and_collection(self, adapter: BasicSerializationAdapter):
        """Test dump_dict includes key and collection when provided."""
        result = adapter.dump_dict(entry=TEST_ENTRY_ONE, key="my-key", collection="my-collection")
        assert result["key"] == "my-key"
        assert result["collection"] == "my-collection"

    def test_dump_dict_with_datetime_format(self):
        """Test dump_dict with datetime format instead of isoformat."""
        adapter = BasicSerializationAdapter(date_format="datetime")
        result = adapter.dump_dict(entry=TEST_ENTRY_ONE)
        assert result["created_at"] == FIXED_DATETIME_ONE
        assert result["expires_at"] == FIXED_DATETIME_ONE_PLUS_10_SECONDS

    def test_load_dict_with_datetime_format(self):
        """Test load_dict with datetime format instead of isoformat."""
        adapter = BasicSerializationAdapter(date_format="datetime")
        data = {
            "created_at": FIXED_DATETIME_ONE,
            "expires_at": FIXED_DATETIME_ONE_PLUS_10_SECONDS,
            "value": TEST_DATA_ONE,
        }
        result = adapter.load_dict(data=data)
        assert result.created_at == FIXED_DATETIME_ONE
        assert result.expires_at == FIXED_DATETIME_ONE_PLUS_10_SECONDS

    def test_dump_json_with_datetime_format_raises_error(self):
        """Test dump_json raises error when date_format is datetime."""
        adapter = BasicSerializationAdapter(date_format="datetime")
        with pytest.raises(SerializationError, match="dump_json is incompatible"):
            adapter.dump_json(entry=TEST_ENTRY_ONE)

    def test_load_dict_with_string_value(self, adapter: BasicSerializationAdapter):
        """Test load_dict with value as JSON string."""
        data = {
            "created_at": FIXED_DATETIME_ONE_ISOFORMAT,
            "expires_at": FIXED_DATETIME_ONE_PLUS_10_SECONDS_ISOFORMAT,
            "value": '{"key": "value"}',
        }
        result = adapter.load_dict(data=data)
        assert result.value == {"key": "value"}

    def test_load_dict_missing_value_raises_error(self, adapter: BasicSerializationAdapter):
        """Test load_dict raises error when value is missing."""
        data = {
            "created_at": FIXED_DATETIME_ONE_ISOFORMAT,
            "expires_at": FIXED_DATETIME_ONE_PLUS_10_SECONDS_ISOFORMAT,
        }
        with pytest.raises(DeserializationError, match="Value field not found"):
            adapter.load_dict(data=data)

    def test_load_dict_invalid_value_type_raises_error(self, adapter: BasicSerializationAdapter):
        """Test load_dict raises error when value is not string or dict."""
        data = {
            "created_at": FIXED_DATETIME_ONE_ISOFORMAT,
            "expires_at": FIXED_DATETIME_ONE_PLUS_10_SECONDS_ISOFORMAT,
            "value": 12345,
        }
        with pytest.raises(DeserializationError, match="Value field is not a string or dictionary"):
            adapter.load_dict(data=data)


class TestKeyMustBe:
    def test_key_missing(self):
        """Test key_must_be returns None when key is missing."""
        result = key_must_be({"other": "value"}, key="missing", expected_type=str)
        assert result is None

    def test_key_wrong_type(self):
        """Test key_must_be raises TypeError when type is wrong."""
        with pytest.raises(TypeError, match="created_at must be a str"):
            key_must_be({"created_at": 12345}, key="created_at", expected_type=str)

    def test_key_correct_type(self):
        """Test key_must_be returns value when type is correct."""
        result = key_must_be({"created_at": "2025-01-01"}, key="created_at", expected_type=str)
        assert result == "2025-01-01"


class TestParseDatetimeStr:
    def test_valid_datetime(self):
        """Test parse_datetime_str with valid datetime string."""
        result = parse_datetime_str("2025-01-01T00:00:00+00:00")
        assert result == FIXED_DATETIME_ONE

    def test_invalid_datetime(self):
        """Test parse_datetime_str raises error for invalid string."""
        with pytest.raises(DeserializationError, match="Invalid datetime string"):
            parse_datetime_str("not-a-datetime")
