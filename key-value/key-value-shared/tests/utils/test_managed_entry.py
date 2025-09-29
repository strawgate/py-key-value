from datetime import datetime, timezone
from typing import Any

import pytest
from key_value.shared_test.cases import SIMPLE_TEST_DATA_ARGNAMES, SIMPLE_TEST_DATA_ARGVALUES, SIMPLE_TEST_DATA_IDS

from key_value.shared.utils.managed_entry import dump_to_json, load_from_json

FIXED_DATETIME = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
FIXED_DATETIME_STRING = FIXED_DATETIME.isoformat()


@pytest.mark.parametrize(
    argnames=SIMPLE_TEST_DATA_ARGNAMES,
    argvalues=SIMPLE_TEST_DATA_ARGVALUES,
    ids=SIMPLE_TEST_DATA_IDS,
)
def test_dump_to_json(data: dict[str, Any], json: str):
    assert dump_to_json(data) == json


@pytest.mark.parametrize(
    argnames=SIMPLE_TEST_DATA_ARGNAMES,
    argvalues=SIMPLE_TEST_DATA_ARGVALUES,
    ids=SIMPLE_TEST_DATA_IDS,
)
def test_load_from_json(data: dict[str, Any], json: str):
    assert load_from_json(json) == data


@pytest.mark.parametrize(
    argnames=SIMPLE_TEST_DATA_ARGNAMES,
    argvalues=SIMPLE_TEST_DATA_ARGVALUES,
    ids=SIMPLE_TEST_DATA_IDS,
)
def test_roundtrip_json(data: dict[str, Any], json: str):
    dumped_json: str = dump_to_json(data)
    assert dumped_json == json
    assert load_from_json(dumped_json) == data
