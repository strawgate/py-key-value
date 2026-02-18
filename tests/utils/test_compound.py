import pytest

from key_value.aio._utils.compound import (
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


def test_compound_and_uncompound_string_default_separator() -> None:
    combined = compound_string(first="collection", second="key")
    assert combined == "collection::key"
    assert uncompound_string(string=combined) == ("collection", "key")


def test_compound_and_uncompound_string_custom_separator() -> None:
    combined = compound_string(first="alpha", second="beta", separator="--")
    assert combined == "alpha--beta"
    assert uncompound_string(string=combined, separator="--") == ("alpha", "beta")


def test_uncompound_string_raises_when_missing_separator() -> None:
    with pytest.raises(TypeError, match="not a compound identifier"):
        uncompound_string(string="no-separator", separator="::")


def test_uncompound_strings_returns_pairs() -> None:
    values = ["one::a", "two::b"]
    assert uncompound_strings(strings=values) == [("one", "a"), ("two", "b")]


def test_compound_key_roundtrip() -> None:
    combined = compound_key(collection="users", key="alice")
    assert combined == "users::alice"
    assert uncompound_key(key=combined) == ("users", "alice")


def test_prefix_key_roundtrip() -> None:
    prefixed = prefix_key(key="alice", prefix="team")
    assert prefixed == "team__alice"
    assert unprefix_key(key=prefixed, prefix="team") == "alice"


def test_unprefix_key_raises_for_missing_prefix() -> None:
    with pytest.raises(ValueError, match="not prefixed"):
        unprefix_key(key="alice", prefix="team")


def test_prefix_collection_roundtrip() -> None:
    prefixed = prefix_collection(collection="users", prefix="team")
    assert prefixed == "team__users"
    assert unprefix_collection(collection=prefixed, prefix="team") == "users"


def test_unprefix_collection_raises_for_missing_prefix() -> None:
    with pytest.raises(ValueError, match="not prefixed"):
        unprefix_collection(collection="users", prefix="team")


def test_get_collections_from_compound_keys_returns_unique_collections() -> None:
    compound_keys = ["users::a", "users::b", "admins::c"]
    collections = get_collections_from_compound_keys(compound_keys=compound_keys)
    assert set(collections) == {"users", "admins"}


def test_get_keys_from_compound_keys_filters_by_collection() -> None:
    compound_keys = ["users::a", "admins::b", "users::c"]
    assert get_keys_from_compound_keys(compound_keys=compound_keys, collection="users") == ["a", "c"]
