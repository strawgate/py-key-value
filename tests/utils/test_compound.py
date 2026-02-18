import pytest

from key_value.aio._utils.compound import (
    DEFAULT_COMPOUND_SEPARATOR,
    DEFAULT_PREFIX_SEPARATOR,
    compound_key,
    compound_string,
    get_collections_from_compound_keys,
    get_keys_from_compound_keys,
    prefix_collection,
    prefix_key,
    uncompound_key,
    uncompound_string,
    uncompound_strings,
    unprefix_collection,
    unprefix_key,
)


def test_compound_string_roundtrip_default():
    compound = compound_string("collection", "key")
    assert compound == f"collection{DEFAULT_COMPOUND_SEPARATOR}key"
    assert uncompound_string(compound) == ("collection", "key")


def test_uncompound_string_invalid():
    with pytest.raises(TypeError, match="not a compound identifier"):
        uncompound_string("collection-key")


def test_uncompound_strings_custom_separator():
    compound_a = compound_string("a", "b", separator="|")
    compound_c = compound_string("c", "d", separator="|")
    assert uncompound_strings([compound_a, compound_c], separator="|") == [("a", "b"), ("c", "d")]


def test_compound_key_roundtrip():
    compound = compound_key(collection="users", key="u1")
    assert compound == f"users{DEFAULT_COMPOUND_SEPARATOR}u1"
    assert uncompound_key(compound) == ("users", "u1")


def test_prefix_key_roundtrip():
    prefixed = prefix_key(key="value", prefix="prefix")
    assert prefixed == f"prefix{DEFAULT_PREFIX_SEPARATOR}value"
    assert unprefix_key(prefixed, prefix="prefix") == "value"


def test_unprefix_key_invalid():
    with pytest.raises(ValueError, match="not prefixed"):
        unprefix_key("other__value", prefix="prefix")


def test_prefix_collection_roundtrip():
    prefixed = prefix_collection(collection="items", prefix="prefix")
    assert prefixed == f"prefix{DEFAULT_PREFIX_SEPARATOR}items"
    assert unprefix_collection(prefixed, prefix="prefix") == "items"


def test_unprefix_collection_invalid():
    with pytest.raises(ValueError, match="not prefixed"):
        unprefix_collection("other__items", prefix="prefix")


def test_compound_key_helpers():
    compound_keys = [
        compound_string("users", "u1"),
        compound_string("users", "u2"),
        compound_string("orders", "o1"),
    ]
    assert set(get_collections_from_compound_keys(compound_keys)) == {"users", "orders"}
    assert get_keys_from_compound_keys(compound_keys, collection="users") == ["u1", "u2"]
