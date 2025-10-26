import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

FIXED_DATETIME = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
FIXED_TIME = FIXED_DATETIME.time()
FIXED_UUID = UUID("12345678-1234-5678-1234-567812345678")


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

# Datetime cases (serialized as ISO format strings)
DATETIME_CASE: Case = Case(
    name="datetime",
    data={"datetime_key": FIXED_DATETIME.isoformat()},
    json='{"datetime_key": "2025-01-01T00:00:00+00:00"}',
)
DATE_CASE: Case = Case(
    name="date",
    data={"date_key": FIXED_DATETIME.date().isoformat()},
    json='{"date_key": "2025-01-01"}',
)
TIME_CASE: Case = Case(
    name="time",
    data={"time_key": FIXED_TIME.isoformat()},
    json='{"time_key": "00:00:00"}',
)

# UUID case (serialized as string)
UUID_CASE: Case = Case(
    name="uuid",
    data={"uuid_key": str(FIXED_UUID)},
    json='{"uuid_key": "12345678-1234-5678-1234-567812345678"}',
)

# Bytes case (base64 encoded)
BYTES_VALUE = b"hello world"
BYTES_CASE: Case = Case(
    name="bytes",
    data={"bytes_key": base64.b64encode(BYTES_VALUE).decode("ascii")},
    json=f'{{"bytes_key": "{base64.b64encode(BYTES_VALUE).decode("ascii")}"}}',
)

# Tuple case (serializes as list in JSON)
TUPLE_CASE: Case = Case(
    name="tuple",
    data={"tuple_key": [1, "two", 3.0]},
    json='{"tuple_key": [1, "two", 3.0]}',
)

# Set case (serializes as sorted list in JSON)
SET_CASE: Case = Case(
    name="set",
    data={"set_key": [1, 2, 3]},
    json='{"set_key": [1, 2, 3]}',
)

# Deeply nested structure cases
DEEP_NESTED_CASE: Case = Case(
    name="deep-nested",
    data={"level1": {"level2": {"level3": {"level4": {"value": "deep"}}}}},
    json='{"level1": {"level2": {"level3": {"level4": {"value": "deep"}}}}}',
)

NESTED_LIST_DICT_CASE: Case = Case(
    name="nested-list-dict",
    data={"items": [{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}]},
    json='{"items": [{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}]}',
)

NESTED_DICT_LIST_CASE: Case = Case(
    name="nested-dict-list",
    data={"categories": {"fruits": ["apple", "banana"], "vegetables": ["carrot", "broccoli"]}},
    json='{"categories": {"fruits": ["apple", "banana"], "vegetables": ["carrot", "broccoli"]}}',
)

MIXED_NESTED_CASE: Case = Case(
    name="mixed-nested",
    data={"outer": {"inner": [{"nested": [1, 2, 3]}]}},
    json='{"outer": {"inner": [{"nested": [1, 2, 3]}]}}',
)

# Edge cases
EMPTY_STRING_CASE: Case = Case(
    name="empty-string",
    data={"empty_key": ""},
    json='{"empty_key": ""}',
)

EMPTY_LIST_CASE: Case = Case(
    name="empty-list",
    data={"empty_list_key": []},
    json='{"empty_list_key": []}',
)

EMPTY_DICT_CASE: Case = Case(
    name="empty-dict",
    data={"empty_dict_key": {}},
    json='{"empty_dict_key": {}}',
)

UNICODE_CASE: Case = Case(
    name="unicode",
    data={"unicode_key": "Hello 世界 🌍 émojis"},
    json='{"unicode_key": "Hello 世界 🌍 émojis"}',
)

SPECIAL_CHARS_CASE: Case = Case(
    name="special-chars",
    data={"special_key": "Line1\nLine2\tTabbed"},
    json='{"special_key": "Line1\\nLine2\\tTabbed"}',
)

# Zero values
ZERO_INT_CASE: Case = Case(
    name="zero-int",
    data={"zero_int_key": 0},
    json='{"zero_int_key": 0}',
)

ZERO_FLOAT_CASE: Case = Case(
    name="zero-float",
    data={"zero_float_key": 0.0},
    json='{"zero_float_key": 0.0}',
)

NEGATIVE_INT_CASE: Case = Case(
    name="negative-int",
    data={"negative_int_key": -42},
    json='{"negative_int_key": -42}',
)

NEGATIVE_FLOAT_CASE: Case = Case(
    name="negative-float",
    data={"negative_float_key": -3.14},
    json='{"negative_float_key": -3.14}',
)

# Large collection case
LARGE_LIST_CASE: Case = Case(
    name="large-list",
    data={"large_list": list(range(1000))},
    json=f'{{"large_list": {json.dumps(list(range(1000)))}}}',
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
        DICT_CASE_ONE,
        DICT_CASE_TWO,
        DICT_CASE_THREE,
        LIST_CASE_ONE,
        LIST_CASE_TWO,
        LIST_CASE_THREE,
        DATETIME_CASE,
        DATE_CASE,
        TIME_CASE,
        UUID_CASE,
        BYTES_CASE,
        TUPLE_CASE,
        SET_CASE,
        DEEP_NESTED_CASE,
        NESTED_LIST_DICT_CASE,
        NESTED_DICT_LIST_CASE,
        MIXED_NESTED_CASE,
        EMPTY_STRING_CASE,
        EMPTY_LIST_CASE,
        EMPTY_DICT_CASE,
        UNICODE_CASE,
        SPECIAL_CHARS_CASE,
        ZERO_INT_CASE,
        ZERO_FLOAT_CASE,
        NEGATIVE_INT_CASE,
        NEGATIVE_FLOAT_CASE,
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
        DICT_CASE_ONE,
        DICT_CASE_TWO,
        DICT_CASE_THREE,
        LIST_CASE_ONE,
        LIST_CASE_TWO,
        LIST_CASE_THREE,
        DATETIME_CASE,
        DATE_CASE,
        TIME_CASE,
        UUID_CASE,
        BYTES_CASE,
        TUPLE_CASE,
        SET_CASE,
        DEEP_NESTED_CASE,
        NESTED_LIST_DICT_CASE,
        NESTED_DICT_LIST_CASE,
        MIXED_NESTED_CASE,
        EMPTY_STRING_CASE,
        EMPTY_LIST_CASE,
        EMPTY_DICT_CASE,
        UNICODE_CASE,
        SPECIAL_CHARS_CASE,
        ZERO_INT_CASE,
        ZERO_FLOAT_CASE,
        NEGATIVE_INT_CASE,
        NEGATIVE_FLOAT_CASE,
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
        DICT_CASE_ONE,
        DICT_CASE_TWO,
        DICT_CASE_THREE,
        LIST_CASE_ONE,
        LIST_CASE_TWO,
        LIST_CASE_THREE,
        DATETIME_CASE,
        DATE_CASE,
        TIME_CASE,
        UUID_CASE,
        BYTES_CASE,
        TUPLE_CASE,
        SET_CASE,
        DEEP_NESTED_CASE,
        NESTED_LIST_DICT_CASE,
        NESTED_DICT_LIST_CASE,
        MIXED_NESTED_CASE,
        EMPTY_STRING_CASE,
        EMPTY_LIST_CASE,
        EMPTY_DICT_CASE,
        UNICODE_CASE,
        SPECIAL_CHARS_CASE,
        ZERO_INT_CASE,
        ZERO_FLOAT_CASE,
        NEGATIVE_INT_CASE,
        NEGATIVE_FLOAT_CASE,
    ]
]

LARGE_TEST_DATA_DATA: list[dict[str, Any]] = [
    case.data
    for case in [
        LARGE_STRING_CASE,
        LARGE_INT_CASE,
        LARGE_FLOAT_CASE,
        LARGE_LIST_CASE,
    ]
]
LARGE_TEST_DATA_JSON: list[str] = [
    case.json
    for case in [
        LARGE_STRING_CASE,
        LARGE_INT_CASE,
        LARGE_FLOAT_CASE,
        LARGE_LIST_CASE,
    ]
]
LARGE_TEST_DATA_ARGNAMES: tuple[str, str] = ("data", "json")
LARGE_TEST_DATA_ARGVALUES: list[tuple[dict[str, Any], str]] = list(zip(LARGE_TEST_DATA_DATA, LARGE_TEST_DATA_JSON, strict=True))
LARGE_TEST_DATA_IDS: list[str] = [
    case.name
    for case in [
        LARGE_STRING_CASE,
        LARGE_INT_CASE,
        LARGE_FLOAT_CASE,
        LARGE_LIST_CASE,
    ]
]

__all__ = [
    "LARGE_TEST_DATA_ARGNAMES",
    "LARGE_TEST_DATA_ARGVALUES",
    "LARGE_TEST_DATA_IDS",
    "SIMPLE_TEST_DATA_ARGNAMES",
    "SIMPLE_TEST_DATA_ARGVALUES",
    "SIMPLE_TEST_DATA_IDS",
]
