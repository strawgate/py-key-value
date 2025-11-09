from datetime import datetime, timedelta, timezone

import pytest
from inline_snapshot import snapshot

from key_value.shared.utils.managed_entry import ManagedEntry
from key_value.shared.utils.serialization import BasicSerializationAdapter

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
