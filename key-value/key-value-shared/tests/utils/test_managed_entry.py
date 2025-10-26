from datetime import datetime, timezone
from typing import Any

from key_value.shared_test.cases import SIMPLE_CASES, PositiveCases

from key_value.shared.utils.managed_entry import dump_to_json, load_from_json

FIXED_DATETIME = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
FIXED_DATETIME_STRING = FIXED_DATETIME.isoformat()


@PositiveCases.parametrize(cases=SIMPLE_CASES)
def test_dump_to_json(data: dict[str, Any], json: str, round_trip: dict[str, Any]):
    assert dump_to_json(data) == json


@SIMPLE_CASES.parametrize()
def test_load_from_json(data: dict[str, Any], json: str, round_trip: dict[str, Any]):
    assert load_from_json(json) == data


@SIMPLE_CASES.parametrize()
def test_roundtrip_json(data: dict[str, Any], json: str, round_trip: dict[str, Any]):
    dumped_json: str = dump_to_json(data)
    assert dumped_json == json
    assert load_from_json(dumped_json) == round_trip
