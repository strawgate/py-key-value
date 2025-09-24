from typing import Any

SIMPLE_CASE: dict[str, Any] = {
    "key_1": "value_1",
    "key_2": 1,
    "key_3": 1.0,
    "key_4": [1, 2, 3],
    "key_5": {"nested": "value"},
    "key_6": True,
    "key_7": False,
    "key_8": None,
}

SIMPLE_CASE_JSON: str = '{"key_1": "value_1", "key_2": 1, "key_3": 1.0, "key_4": [1, 2, 3], "key_5": {"nested": "value"}, "key_6": true, "key_7": false, "key_8": null}'

DICTIONARY_TO_JSON_TEST_CASES: list[tuple[dict[str, Any], str]] = [
    ({"key": "value"}, '{"key": "value"}'),
    ({"key": 1}, '{"key": 1}'),
    ({"key": 1.0}, '{"key": 1.0}'),
    ({"key": [1, 2, 3]}, '{"key": [1, 2, 3]}'),
    ({"key": {"nested": "value"}}, '{"key": {"nested": "value"}}'),
    ({"key": True}, '{"key": true}'),
    ({"key": False}, '{"key": false}'),
    ({"key": None}, '{"key": null}'),
]

DICTIONARY_TO_JSON_TEST_CASES_NAMES: list[str] = [
    "string",
    "int",
    "float",
    "list",
    "dict",
    "bool-false",
    "bool-true",
    "null",
]

OBJECT_TEST_CASES: list[dict[str, Any]] = [test_case[0] for test_case in DICTIONARY_TO_JSON_TEST_CASES]

JSON_TEST_CASES: list[str] = [test_case[1] for test_case in DICTIONARY_TO_JSON_TEST_CASES]
