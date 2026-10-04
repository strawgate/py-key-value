from dataclasses import dataclass

import pytest
from pydantic import BaseModel

from key_value.aio.adapters.base_model import BaseModelAdapter
from key_value.aio.adapters.dataclass import DataclassAdapter
from key_value.aio.adapters.pydantic import PydanticAdapter
from key_value.aio.errors import DeserializationError
from key_value.aio.stores.memory import MemoryStore


class NumberModel(BaseModel):
    value: int


@dataclass
class NumberDataclass:
    value: int


NumberAdapter = PydanticAdapter[NumberModel] | BaseModelAdapter[NumberModel] | DataclassAdapter[NumberDataclass]


def make_adapter(kind: str, store: MemoryStore, *, raise_on_validation_error: bool) -> NumberAdapter:
    if kind == "pydantic":
        return PydanticAdapter(store, NumberModel, default_collection="test", raise_on_validation_error=raise_on_validation_error)
    if kind == "base_model":
        return BaseModelAdapter(store, NumberModel, default_collection="test", raise_on_validation_error=raise_on_validation_error)
    assert kind == "dataclass"
    return DataclassAdapter(store, NumberDataclass, default_collection="test", raise_on_validation_error=raise_on_validation_error)


@pytest.mark.parametrize("kind", ["pydantic", "base_model", "dataclass"])
@pytest.mark.parametrize("ttl", [None, 60])
async def test_single_and_bulk_ttl_validation_failure_parity(kind: str, ttl: float | None):
    store = MemoryStore()
    adapter = make_adapter(kind, store, raise_on_validation_error=False)
    await store.put("valid", {"value": 0}, collection="test", ttl=ttl)
    await store.put("invalid", {"value": "not-an-int"}, collection="test", ttl=ttl)

    assert await adapter.ttl("invalid") == (None, None)
    assert await adapter.ttl("missing") == (None, None)
    results = await adapter.ttl_many(["valid", "invalid", "missing", "valid"])
    assert len(results) == 4
    assert results[1:3] == [(None, None), (None, None)]

    for model, remaining in (await adapter.ttl("valid"), results[0], results[3]):
        assert model is not None
        assert model.value == 0
        if ttl is None:
            assert remaining is None
        else:
            assert remaining is not None
            assert 0 < remaining <= ttl

    assert await adapter.ttl_many([]) == []


@pytest.mark.parametrize("kind", ["pydantic", "base_model", "dataclass"])
async def test_single_and_bulk_ttl_keep_raising_validation_errors(kind: str):
    store = MemoryStore()
    adapter = make_adapter(kind, store, raise_on_validation_error=True)
    await store.put("valid", {"value": 0}, collection="test", ttl=60)
    await store.put("invalid", {"value": "not-an-int"}, collection="test", ttl=60)

    with pytest.raises(DeserializationError):
        await adapter.ttl("invalid")
    with pytest.raises(DeserializationError):
        await adapter.ttl_many(["valid", "invalid"])


@pytest.mark.parametrize("payload", [{"items": "not-an-int"}, {"wrong": 0}])
async def test_bulk_ttl_preserves_zero_and_normalizes_invalid_wrapped_payloads(payload: dict[str, str | int]):
    store = MemoryStore()
    adapter = PydanticAdapter(store, int)
    await store.put("valid", {"items": 0}, ttl=60)
    await store.put("invalid", payload, ttl=60)

    assert await adapter.ttl("invalid") == (None, None)
    results = await adapter.ttl_many(["valid", "invalid"])
    assert results[0][0] == 0
    assert results[0][1] is not None
    assert 0 < results[0][1] <= 60
    assert results[1] == (None, None)


async def test_bulk_ttl_matches_existing_single_ttl_policy_for_valid_none():
    store = MemoryStore()
    adapter = PydanticAdapter[None](store, type(None))
    await adapter.put("key", None, ttl=60)

    # The single-value API already treats a validated None as no model value.
    assert await adapter.ttl("key") == (None, None)
    assert await adapter.ttl_many(["key"]) == [(None, None)]
