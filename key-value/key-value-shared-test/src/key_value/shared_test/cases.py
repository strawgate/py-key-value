import base64
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Generic, TypeVar
from uuid import UUID

import pytest
from _pytest.mark.structures import MarkDecorator
from key_value.shared.errors.key_value import SerializationError
from typing_extensions import Self

FIXED_DATETIME = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
FIXED_TIME = FIXED_DATETIME.time()
FIXED_UUID = UUID("12345678-1234-5678-1234-567812345678")


@dataclass
class BaseCase:
    name: str
    data: dict[str, Any]
    round_trip: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.round_trip:
            self.round_trip = self.data


@dataclass
class Case(BaseCase):
    json: str = field(default_factory=str)


@dataclass
class NegativeCase(BaseCase):
    error: type[Exception] = field(default_factory=lambda: SerializationError)


BC = TypeVar("BC", bound=BaseCase)


@dataclass
class Cases(Generic[BC]):
    case_type: str
    cases: list[BC] = field(default_factory=list)

    def __init__(self, *args: BC, case_type: str):
        self.case_type = case_type
        self.cases = list(args)

    def add_case(self, case: BC) -> Self:
        self.cases.append(case)
        return self

    def to_ids(self) -> list[str]:
        return [case.name for case in self.cases]


@dataclass
class PositiveCases(Cases[Case]):
    def __init__(self, *args: Case, case_type: str):
        super().__init__(*args, case_type=case_type)

    @classmethod
    def to_argnames(cls) -> tuple[str, str, str]:
        return ("data", "json", "round_trip")

    def to_argvalues(self) -> list[tuple[dict[str, Any], str, dict[str, Any]]]:
        return [(case.data, case.json, case.round_trip) for case in self.cases]

    @classmethod
    def parametrize(cls, cases: list[Self]) -> MarkDecorator:
        argnames = cls.to_argnames()
        argvalues = [row for case_group in cases for row in case_group.to_argvalues()]
        ids = [f"{case_group.case_type}-{case.name}" for case_group in cases for case in case_group.cases]
        return pytest.mark.parametrize(argnames=argnames, argvalues=argvalues, ids=ids)


@dataclass
class NegativeCases(Cases[NegativeCase]):
    def __init__(self, *args: NegativeCase, case_type: str):
        self.case_type = case_type
        super().__init__(*args, case_type=case_type)

    @classmethod
    def to_argnames(cls) -> tuple[str, str]:
        return ("data", "error")

    def to_argvalues(self) -> list[tuple[dict[str, Any], type[Exception]]]:
        return [(case.data, case.error) for case in self.cases]

    @classmethod
    def parametrize(cls, cases: list[Self]) -> MarkDecorator:
        argnames = cls.to_argnames()
        argvalues = [row for case_group in cases for row in case_group.to_argvalues()]
        ids = [f"{case_group.case_type}-{case.name}" for case_group in cases for case in case_group.cases]
        return pytest.mark.parametrize(argnames=argnames, argvalues=argvalues, ids=ids)


# Add null test cases
NULL_CASES: PositiveCases = PositiveCases(
    Case(name="value", data={"null_key": None}, json='{"null_key": null}'),
    Case(name="list", data={"str_key": [None, None, None]}, json='{"str_key": [null, null, null]}'),
    Case(
        name="nested-dict-values",
        data={"str_key": {"str_key_2": None, "str_key_3": None}},
        json='{"str_key": {"str_key_2": null, "str_key_3": null}}',
    ),
    Case(name="no-implicit-serialization-in-keys", data={"null": "str_value"}, json='{"null": "str_value"}'),
    Case(name="no-implicit-serialization-in-values", data={"str_key": "null"}, json='{"str_key": "null"}'),
    case_type="null",
)

NEGATIVE_NULL_CASES: NegativeCases = NegativeCases(case_type="null")

BOOLEAN_CASES: PositiveCases = PositiveCases(
    Case(name="value-true", data={"bool_true_key": True}, json='{"bool_true_key": true}'),
    Case(name="value-false", data={"bool_false_key": False}, json='{"bool_false_key": false}'),
    Case(name="list", data={"str_key": [True, False, True]}, json='{"str_key": [true, false, true]}'),
    Case(
        name="nested-dict-values",
        data={"str_key": {"str_key_2": True, "str_key_3": False}},
        json='{"str_key": {"str_key_2": true, "str_key_3": false}}',
    ),
    Case(name="no-implicit-serialization-in-keys", data={"true": "str_value"}, json='{"true": "str_value"}'),
    Case(name="no-implicit-serialization-in-values", data={"str_key": "true"}, json='{"str_key": "true"}'),
    case_type="boolean",
)

NEGATIVE_BOOLEAN_CASES: NegativeCases = NegativeCases(
    case_type="boolean",
)

INTEGER_CASES: PositiveCases = PositiveCases(
    Case(name="int", data={"int_key": 1}, json='{"int_key": 1}'),
    Case(name="value-negative", data={"negative_int_key": -42}, json='{"negative_int_key": -42}'),
    Case(name="large-value", data={"large_int_key": 1 * 10**18}, json=f'{{"large_int_key": {1 * 10**18}}}'),
    Case(name="no-implicit-serialization-in-keys", data={"1": "str_value"}, json='{"1": "str_value"}'),
    Case(name="no-implicit-serialization-in-values", data={"str_key": "1"}, json='{"str_key": "1"}'),
    case_type="integer",
)

NEGATIVE_INTEGER_CASES: NegativeCases = NegativeCases(
    case_type="integer",
)

FLOAT_CASES: PositiveCases = PositiveCases(
    Case(name="value", data={"float_key": 1.0}, json='{"float_key": 1.0}'),
    Case(name="large-value", data={"large_float_key": 1.23456789012345 * 10**10}, json=f'{{"large_float_key": {1.23456789012345 * 10**10}}}'),
    Case(name="no-implicit-serialization-in-keys", data={"1.0": "str_value"}, json='{"1.0": "str_value"}'),
    Case(name="no-implicit-serialization-in-values", data={"str_key": "1.0"}, json='{"str_key": "1.0"}'),
    case_type="float",
)

NEGATIVE_FLOAT_CASES: NegativeCases = NegativeCases(
    case_type="float",
)

STRING_CASES: PositiveCases = PositiveCases(
    Case(name="string", data={"string_key": "string_value"}, json='{"string_key": "string_value"}'),
    Case(name="1kb-value", data={"string_key": "a" * 1024}, json=f'{{"string_key": "{"a" * 1024}"}}'),
    Case(
        name="unicode",
        data={"unicode_key": "Hello 世界 🌍 émojis"},
        json='{"unicode_key": "Hello \\u4e16\\u754c \\ud83c\\udf0d \\u00e9mojis"}',
    ),
    Case(name="special-chars", data={"special_key": "Line1\nLine2\tTabbed"}, json='{"special_key": "Line1\\nLine2\\tTabbed"}'),
    case_type="string",
)


DATETIME_CASES: PositiveCases = PositiveCases(case_type="datetime")

NEGATIVE_DATETIME_CASES: NegativeCases = NegativeCases(
    NegativeCase(name="datetime-value", data={"str_key": FIXED_DATETIME}),
    NegativeCase(name="date-value", data={"str_key": FIXED_DATETIME.date()}),
    NegativeCase(name="time-value", data={"str_key": FIXED_TIME}),
    case_type="datetime",
)

UUID_CASES: PositiveCases = PositiveCases(
    Case(
        name="no-implicit-conversion-in-keys",
        data={"12345678-1234-5678-1234-567812345678": "str_value"},
        json='{"12345678-1234-5678-1234-567812345678": "str_value"}',
    ),
    Case(
        name="no-implicit-conversion-in-values",
        data={"str_key": "12345678-1234-5678-1234-567812345678"},
        json='{"str_key": "12345678-1234-5678-1234-567812345678"}',
    ),
    case_type="uuid",
)

NEGATIVE_UUID_CASES: NegativeCases = NegativeCases(
    NegativeCase(name="value", data={"str_key": FIXED_UUID}),
    case_type="uuid",
)

B_HELLO_WORLD: bytes = b"hello world"
B64_HELLO_WORLD: str = base64.b64encode(B_HELLO_WORLD).decode(encoding="ascii")

BYTES_CASES: PositiveCases = PositiveCases(
    Case(name="no-implicit-conversion-in-keys", data={B64_HELLO_WORLD: "str_value"}, json=f'{{"{B64_HELLO_WORLD}": "str_value"}}'),
    Case(name="no-implicit-conversion-in-values", data={"str_key": B64_HELLO_WORLD}, json=f'{{"str_key": "{B64_HELLO_WORLD}"}}'),
    case_type="bytes",
)

NEGATIVE_BYTES_CASES: NegativeCases = NegativeCases(
    NegativeCase(name="bytes-value", data={"str_key": B_HELLO_WORLD}),
    case_type="bytes",
)

SAMPLE_TUPLE: tuple[int, str, float] = (1, "two", 3.0)

TUPLE_CASES: PositiveCases = PositiveCases(
    Case(name="tuple", data={"str_key": SAMPLE_TUPLE}, json='{"str_key": [1, "two", 3.0]}'),
    Case(
        name="large-tuple",
        data={"str_key": (1, "two", 3.0) * 1000},
        json=f'{{"str_key": {json.dumps((1, "two", 3.0) * 1000)}}}',
    ),
    case_type="tuple",
)

NEGATIVE_TUPLE_CASES: NegativeCases = NegativeCases(
    NegativeCase(name="value", data={"str_key": SAMPLE_TUPLE}),
    case_type="tuple",
)

SAMPLE_SET: set[int] = {1, 2, 3}

SET_CASES: PositiveCases = PositiveCases(case_type="set")

NEGATIVE_SET_CASES: NegativeCases = NegativeCases(
    NegativeCase(name="small", data={"str_key": SAMPLE_SET}),
    NegativeCase(name="large", data={"large_set_key": set(range(1000))}),
    case_type="set",
)

DEEP_NESTED_CASES: PositiveCases = PositiveCases(
    Case(
        name="simple",
        data={"level1": {"level2": {"level3": {"level4": {"value": "deep"}}}}},
        json='{"level1": {"level2": {"level3": {"level4": {"value": "deep"}}}}}',
    ),
    case_type="deep-nested",
)

NEGATIVE_DEEP_NESTED_CASES: NegativeCases = NegativeCases(case_type="deep-nested")

NESTED_LIST_DICT_CASES: PositiveCases = PositiveCases(
    Case(
        name="nested-list-dict",
        data={"items": [{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}]},
        json='{"items": [{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}]}',
    ),
    Case(
        name="large-nested-list-dict",
        data={"items": [{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}] * 1000},
        json=f'{{"items": {json.dumps([{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}] * 1000)}}}',
    ),
    case_type="nested-list-dict",
)

NEGATIVE_NESTED_LIST_DICT_CASES: NegativeCases = NegativeCases(case_type="nested-list-dict")

NESTED_DICT_LIST_CASES: PositiveCases = PositiveCases(
    Case(
        name="nested-dict-list",
        data={"categories": {"fruits": ["apple", "banana"], "vegetables": ["carrot", "broccoli"]}},
        json='{"categories": {"fruits": ["apple", "banana"], "vegetables": ["carrot", "broccoli"]}}',
    ),
    case_type="nested-dict-list",
)

NEGATIVE_NESTED_DICT_LIST_CASES: NegativeCases = NegativeCases(case_type="nested-dict-list")

LARGE_DATA_CASES: PositiveCases = PositiveCases(
    Case(
        name="string",
        data={"large_string_key": "a" * 10000},
        json=f'{{"large_string_key": "{"a" * 10000}"}}',
    ),
    Case(
        name="int",
        data={"large_int_key": 1 * 10**18},
        json=f'{{"large_int_key": {1 * 10**18}}}',
    ),
    Case(
        name="float",
        data={"large_float_key": 1.23456789012345 * 10**10},
        json=f'{{"large_float_key": {1.23456789012345 * 10**10}}}',
    ),
    Case(
        name="list",
        data={"large_list_key": list(range(1000))},
        json=f'{{"large_list_key": {json.dumps(list(range(1000)))}}}',
    ),
    case_type="large-data",
)

NEGATIVE_LARGE_DATA_CASES: NegativeCases = NegativeCases(
    NegativeCase(name="string", data={"large_string_key": "a" * 10_000_000}, error=SerializationError),
    case_type="large-data",
)

SIMPLE_CASES: list[PositiveCases] = [
    NULL_CASES,
    BOOLEAN_CASES,
    INTEGER_CASES,
    FLOAT_CASES,
    STRING_CASES,
    DATETIME_CASES,
    UUID_CASES,
]

NEGATIVE_SIMPLE_CASES: list[NegativeCases] = [
    NEGATIVE_NULL_CASES,
    NEGATIVE_BOOLEAN_CASES,
    NEGATIVE_INTEGER_CASES,
    NEGATIVE_FLOAT_CASES,
    NEGATIVE_DATETIME_CASES,
    NEGATIVE_UUID_CASES,
]
