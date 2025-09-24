from datetime import datetime, timezone
from typing import Any

import pytest

from kv_store_adapter.stores.utils.managed_entry import dump_to_json, load_from_json
from tests.cases import DICTIONARY_TO_JSON_TEST_CASES, DICTIONARY_TO_JSON_TEST_CASES_NAMES

FIXED_DATETIME = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
FIXED_DATETIME_STRING = FIXED_DATETIME.isoformat()


@pytest.mark.parametrize(
    argnames=("obj", "expected"),
    argvalues=DICTIONARY_TO_JSON_TEST_CASES,
    ids=DICTIONARY_TO_JSON_TEST_CASES_NAMES,
)
def test_dump_to_json(obj: dict[str, Any], expected: str):
    assert dump_to_json(obj) == expected


@pytest.mark.parametrize(
    argnames=("obj", "expected"),
    argvalues=DICTIONARY_TO_JSON_TEST_CASES,
    ids=DICTIONARY_TO_JSON_TEST_CASES_NAMES,
)
def test_load_from_json(obj: dict[str, Any], expected: str):
    assert load_from_json(expected) == obj


@pytest.mark.parametrize(
    argnames=("obj", "expected"),
    argvalues=DICTIONARY_TO_JSON_TEST_CASES,
    ids=DICTIONARY_TO_JSON_TEST_CASES_NAMES,
)
def test_roundtrip_json(obj: dict[str, Any], expected: str):
    dumped_json: str = dump_to_json(obj)
    assert dumped_json == expected
    assert load_from_json(dumped_json) == obj
