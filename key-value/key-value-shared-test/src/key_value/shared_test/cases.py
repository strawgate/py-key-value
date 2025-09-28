from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

FIXED_DATETIME = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
FIXED_TIME = FIXED_DATETIME.time()


@dataclass
class Case:
    name: str
    data: dict[str, Any]
    json: str


NULL_CASE: Case = Case(name="null", data={"null_key": None}, json='{"null_key": null}')

BOOL_TRUE_CASE: Case = Case(name="bool-true", data={"bool_true_key": True}, json='{"bool_true_key": true}')
BOOL_FALSE_CASE: Case = Case(name="bool-false", data={"bool_false_key": False}, json='{"bool_false_key": false}')

INT_CASE: Case = Case(name="int", data={"int_key": 1}, json='{"int_key": 1}')
LARGE_INT_CASE: Case = Case(name="large-int", data={"large_int_key": 1 * 10**18}, json=f'{{"large_int_key": {1 * 10**18}}}')

FLOAT_CASE: Case = Case(name="float", data={"float_key": 1.0}, json='{"float_key": 1.0}')
LARGE_FLOAT_CASE: Case = Case(name="large-float", data={"large_float_key": 1.0 * 10**63}, json=f'{{"large_float_key": {1.0 * 10**63}}}')

STRING_CASE: Case = Case(name="string", data={"string_key": "string_value"}, json='{"string_key": "string_value"}')
LARGE_STRING_CASE: Case = Case(name="large-string", data={"large_string_key": "a" * 10000}, json=f'{{"large_string_key": "{"a" * 10000}"}}')

DICT_CASE_ONE: Case = Case(name="dict-one", data={"dict_key_1": {"nested": "value"}}, json='{"dict_key_1": {"nested": "value"}}')
DICT_CASE_TWO: Case = Case(
    name="dict-two",
    data={"dict_key_1": {"nested": "value"}, "dict_key_2": {"nested": "value"}},
    json='{"dict_key_1": {"nested": "value"}, "dict_key_2": {"nested": "value"}}',
)
DICT_CASE_THREE: Case = Case(
    name="dict-three",
    data={"dict_key_1": {"nested": "value"}, "dict_key_2": {"nested": "value"}, "dict_key_3": {"nested": "value"}},
    json='{"dict_key_1": {"nested": "value"}, "dict_key_2": {"nested": "value"}, "dict_key_3": {"nested": "value"}}',
)

LIST_CASE_ONE: Case = Case(name="list", data={"list_key": [1, 2, 3]}, json='{"list_key": [1, 2, 3]}')
LIST_CASE_TWO: Case = Case(
    name="list-two", data={"list_key_1": [1, 2, 3], "list_key_2": [1, 2, 3]}, json='{"list_key_1": [1, 2, 3], "list_key_2": [1, 2, 3]}'
)
LIST_CASE_THREE: Case = Case(
    name="list-three", data={"list_key_1": [1, True, 3.0, "string"]}, json='{"list_key_1": [1, true, 3.0, "string"]}'
)


TEST_CASE_DATA: list[dict[str, Any]] = [
    case.data
    for case in [
        NULL_CASE,
        BOOL_TRUE_CASE,
        BOOL_FALSE_CASE,
        INT_CASE,
        LARGE_INT_CASE,
        FLOAT_CASE,
        LARGE_FLOAT_CASE,
        STRING_CASE,
        LARGE_STRING_CASE,
        DICT_CASE_ONE,
        DICT_CASE_TWO,
        DICT_CASE_THREE,
        LIST_CASE_ONE,
        LIST_CASE_TWO,
        LIST_CASE_THREE,
    ]
]
TEST_CASE_JSON: list[str] = [
    case.json
    for case in [
        NULL_CASE,
        BOOL_TRUE_CASE,
        BOOL_FALSE_CASE,
        INT_CASE,
        LARGE_INT_CASE,
        FLOAT_CASE,
        LARGE_FLOAT_CASE,
        STRING_CASE,
        LARGE_STRING_CASE,
        DICT_CASE_ONE,
        DICT_CASE_TWO,
        DICT_CASE_THREE,
        LIST_CASE_ONE,
        LIST_CASE_TWO,
        LIST_CASE_THREE,
    ]
]

SIMPLE_TEST_DATA_ARGNAMES: tuple[str, str] = ("data", "json")
SIMPLE_TEST_DATA_ARGVALUES: list[tuple[dict[str, Any], str]] = list(zip(TEST_CASE_DATA, TEST_CASE_JSON, strict=True))
SIMPLE_TEST_DATA_IDS: list[str] = [
    case.name
    for case in [
        NULL_CASE,
        BOOL_TRUE_CASE,
        BOOL_FALSE_CASE,
        INT_CASE,
        LARGE_INT_CASE,
        FLOAT_CASE,
        LARGE_FLOAT_CASE,
        STRING_CASE,
        LARGE_STRING_CASE,
        DICT_CASE_ONE,
        DICT_CASE_TWO,
        DICT_CASE_THREE,
        LIST_CASE_ONE,
        LIST_CASE_TWO,
        LIST_CASE_THREE,
    ]
]

__all__ = ["SIMPLE_TEST_DATA_ARGNAMES", "SIMPLE_TEST_DATA_ARGVALUES", "SIMPLE_TEST_DATA_IDS"]
