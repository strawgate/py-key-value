from datetime import datetime, timezone
from typing import Any

from key_value.aio.utils.managed_entry import dump_to_json, load_from_json
from tests.shared.cases import SIMPLE_CASES, PositiveCases

FIXED_DATETIME = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
FIXED_DATETIME_STRING = FIXED_DATETIME.isoformat()


@PositiveCases.parametrize(cases=SIMPLE_CASES)
def test_dump_to_json(data: dict[str, Any], json: str, round_trip: dict[str, Any]):
    """Test that the dump_to_json function dumps the data to the matching JSON string"""
    assert dump_to_json(data) == json


@PositiveCases.parametrize(cases=SIMPLE_CASES)
def test_load_from_json(data: dict[str, Any], json: str, round_trip: dict[str, Any]):
    """Test that the load_from_json function loads the data (round-trip) from the matching JSON string"""
    assert load_from_json(json) == round_trip


@PositiveCases.parametrize(cases=SIMPLE_CASES)
def test_roundtrip_json(data: dict[str, Any], json: str, round_trip: dict[str, Any]):
    """Test that the dump_to_json and load_from_json functions roundtrip the data"""
    dumped_json: str = dump_to_json(data)
    assert dumped_json == json
    assert load_from_json(dumped_json) == round_trip
