"""Tests for compound key utilities."""

import pytest

from key_value.shared.utils.compound import (
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


class TestCompoundString:
    def test_compound_string_default_separator(self):
        """Test compound_string with default separator."""
        result = compound_string("first", "second")
        assert result == f"first{DEFAULT_COMPOUND_SEPARATOR}second"

    def test_compound_string_custom_separator(self):
        """Test compound_string with custom separator."""
        result = compound_string("first", "second", separator="__")
        assert result == "first__second"

    def test_uncompound_string_default_separator(self):
        """Test uncompound_string with default separator."""
        first, second = uncompound_string(f"first{DEFAULT_COMPOUND_SEPARATOR}second")
        assert first == "first"
        assert second == "second"

    def test_uncompound_string_custom_separator(self):
        """Test uncompound_string with custom separator."""
        first, second = uncompound_string("first__second", separator="__")
        assert first == "first"
        assert second == "second"

    def test_uncompound_string_invalid(self):
        """Test uncompound_string with invalid string."""
        with pytest.raises(TypeError, match="not a compound identifier"):
            uncompound_string("not_compound")

    def test_uncompound_string_multiple_separators(self):
        """Test uncompound_string with multiple separators (only splits on first)."""
        first, second = uncompound_string(f"first{DEFAULT_COMPOUND_SEPARATOR}second{DEFAULT_COMPOUND_SEPARATOR}third")
        assert first == "first"
        assert second == f"second{DEFAULT_COMPOUND_SEPARATOR}third"

    def test_uncompound_strings(self):
        """Test uncompound_strings with multiple strings."""
        strings = [f"a{DEFAULT_COMPOUND_SEPARATOR}1", f"b{DEFAULT_COMPOUND_SEPARATOR}2"]
        result = uncompound_strings(strings)
        assert result == [("a", "1"), ("b", "2")]


class TestCompoundKey:
    def test_compound_key_basic(self):
        """Test compound_key basic usage."""
        result = compound_key(collection="users", key="user1")
        assert result == f"users{DEFAULT_COMPOUND_SEPARATOR}user1"

    def test_compound_key_custom_separator(self):
        """Test compound_key with custom separator."""
        result = compound_key(collection="users", key="user1", separator="__")
        assert result == "users__user1"

    def test_uncompound_key_basic(self):
        """Test uncompound_key basic usage."""
        collection, key = uncompound_key(f"users{DEFAULT_COMPOUND_SEPARATOR}user1")
        assert collection == "users"
        assert key == "user1"

    def test_get_collections_from_compound_keys(self):
        """Test get_collections_from_compound_keys."""
        keys = [
            f"users{DEFAULT_COMPOUND_SEPARATOR}user1",
            f"users{DEFAULT_COMPOUND_SEPARATOR}user2",
            f"posts{DEFAULT_COMPOUND_SEPARATOR}post1",
        ]
        collections = get_collections_from_compound_keys(keys)
        assert set(collections) == {"users", "posts"}

    def test_get_keys_from_compound_keys(self):
        """Test get_keys_from_compound_keys."""
        keys = [
            f"users{DEFAULT_COMPOUND_SEPARATOR}user1",
            f"users{DEFAULT_COMPOUND_SEPARATOR}user2",
            f"posts{DEFAULT_COMPOUND_SEPARATOR}post1",
        ]
        user_keys = get_keys_from_compound_keys(keys, collection="users")
        assert user_keys == ["user1", "user2"]


class TestPrefixFunctions:
    def test_prefix_key_default(self):
        """Test prefix_key with default separator."""
        result = prefix_key(key="mykey", prefix="v1")
        assert result == f"v1{DEFAULT_PREFIX_SEPARATOR}mykey"

    def test_prefix_key_custom(self):
        """Test prefix_key with custom separator."""
        result = prefix_key(key="mykey", prefix="v1", separator=":")
        assert result == "v1:mykey"

    def test_unprefix_key_default(self):
        """Test unprefix_key with default separator."""
        result = unprefix_key(key=f"v1{DEFAULT_PREFIX_SEPARATOR}mykey", prefix="v1")
        assert result == "mykey"

    def test_unprefix_key_invalid(self):
        """Test unprefix_key with invalid key."""
        with pytest.raises(ValueError, match="not prefixed"):
            unprefix_key(key="mykey", prefix="v1")

    def test_prefix_collection_default(self):
        """Test prefix_collection with default separator."""
        result = prefix_collection(collection="mycol", prefix="prod")
        assert result == f"prod{DEFAULT_PREFIX_SEPARATOR}mycol"

    def test_unprefix_collection_default(self):
        """Test unprefix_collection with default separator."""
        result = unprefix_collection(collection=f"prod{DEFAULT_PREFIX_SEPARATOR}mycol", prefix="prod")
        assert result == "mycol"

    def test_unprefix_collection_invalid(self):
        """Test unprefix_collection with invalid collection."""
        with pytest.raises(ValueError, match="not prefixed"):
            unprefix_collection(collection="mycol", prefix="prod")
