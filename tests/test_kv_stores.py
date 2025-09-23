"""
Basic tests for KV Store implementations.
"""

import time
import pytest
from datetime import timedelta

from kv_store_adapter.exceptions import KeyNotFoundError


class TestKVStoreBasics:
    """Test basic KV store operations."""

    def test_set_and_get(self, kv_store):
        """Test setting and getting values."""
        kv_store.set("test_key", "test_value")
        assert kv_store.get("test_key") == "test_value"

    def test_get_nonexistent_key(self, kv_store):
        """Test getting a key that doesn't exist."""
        with pytest.raises(KeyNotFoundError):
            kv_store.get("nonexistent_key")

    def test_delete_existing_key(self, kv_store):
        """Test deleting an existing key."""
        kv_store.set("test_key", "test_value")
        assert kv_store.delete("test_key") is True
        
        with pytest.raises(KeyNotFoundError):
            kv_store.get("test_key")

    def test_delete_nonexistent_key(self, kv_store):
        """Test deleting a key that doesn't exist."""
        assert kv_store.delete("nonexistent_key") is False

    def test_exists(self, kv_store):
        """Test checking if keys exist."""
        assert kv_store.exists("test_key") is False
        
        kv_store.set("test_key", "test_value")
        assert kv_store.exists("test_key") is True
        
        kv_store.delete("test_key")
        assert kv_store.exists("test_key") is False

    def test_keys_listing(self, kv_store):
        """Test listing keys."""
        # Initially empty
        assert kv_store.keys() == []
        
        # Add some keys
        kv_store.set("key1", "value1")
        kv_store.set("key2", "value2")
        kv_store.set("test_key", "test_value")
        
        keys = kv_store.keys()
        assert len(keys) == 3
        assert "key1" in keys
        assert "key2" in keys
        assert "test_key" in keys

    def test_keys_pattern_matching(self, kv_store):
        """Test listing keys with patterns."""
        kv_store.set("user_1", "data1")
        kv_store.set("user_2", "data2") 
        kv_store.set("admin_1", "admin_data")
        
        user_keys = kv_store.keys(pattern="user_*")
        assert len(user_keys) == 2
        assert "user_1" in user_keys
        assert "user_2" in user_keys
        assert "admin_1" not in user_keys


class TestKVStoreNamespaces:
    """Test namespace/collection functionality."""

    def test_namespace_isolation(self, kv_store):
        """Test that namespaces isolate data."""
        kv_store.set("key1", "default_value")
        kv_store.set("key1", "ns1_value", namespace="namespace1")
        kv_store.set("key1", "ns2_value", namespace="namespace2")
        
        assert kv_store.get("key1") == "default_value"
        assert kv_store.get("key1", namespace="namespace1") == "ns1_value"
        assert kv_store.get("key1", namespace="namespace2") == "ns2_value"

    def test_namespace_keys_listing(self, kv_store):
        """Test listing keys within namespaces."""
        kv_store.set("key1", "value1")
        kv_store.set("key2", "value2")
        kv_store.set("key1", "ns_value1", namespace="test_ns")
        kv_store.set("key3", "ns_value3", namespace="test_ns")
        
        default_keys = kv_store.keys()
        assert len(default_keys) == 2
        assert "key1" in default_keys
        assert "key2" in default_keys
        
        ns_keys = kv_store.keys(namespace="test_ns")
        assert len(ns_keys) == 2
        assert "key1" in ns_keys
        assert "key3" in ns_keys

    def test_clear_namespace(self, kv_store):
        """Test clearing all keys in a namespace."""
        kv_store.set("key1", "value1")
        kv_store.set("key2", "value2")
        kv_store.set("key1", "ns_value1", namespace="test_ns")
        kv_store.set("key3", "ns_value3", namespace="test_ns")
        
        # Clear the test namespace
        cleared_count = kv_store.clear_namespace("test_ns")
        assert cleared_count == 2
        
        # Default namespace should be unchanged
        assert len(kv_store.keys()) == 2
        
        # Test namespace should be empty
        assert len(kv_store.keys(namespace="test_ns")) == 0

    def test_list_namespaces(self, kv_store):
        """Test listing available namespaces."""
        # Initially no namespaces (or just default)
        namespaces = kv_store.list_namespaces()
        
        # Add data to different namespaces
        kv_store.set("key1", "value1")  # default namespace
        kv_store.set("key1", "value1", namespace="ns1")
        kv_store.set("key1", "value1", namespace="ns2")
        
        namespaces = kv_store.list_namespaces()
        assert "default" in namespaces
        assert "ns1" in namespaces
        assert "ns2" in namespaces


class TestKVStoreTTL:
    """Test TTL (Time To Live) functionality."""

    def test_ttl_with_seconds(self, kv_store):
        """Test TTL with seconds as integer."""
        kv_store.set("ttl_key", "ttl_value", ttl=2)
        
        # Key should exist initially
        assert kv_store.exists("ttl_key") is True
        assert kv_store.get("ttl_key") == "ttl_value"
        
        # TTL should be approximately 2 seconds
        ttl_remaining = kv_store.ttl("ttl_key")
        assert ttl_remaining is not None
        assert 1.0 <= ttl_remaining <= 2.0

    def test_ttl_with_timedelta(self, kv_store):
        """Test TTL with timedelta object."""
        kv_store.set("ttl_key", "ttl_value", ttl=timedelta(seconds=3))
        
        assert kv_store.exists("ttl_key") is True
        ttl_remaining = kv_store.ttl("ttl_key")
        assert ttl_remaining is not None
        assert 2.0 <= ttl_remaining <= 3.0

    def test_ttl_expiration(self, kv_store):
        """Test that keys expire after TTL."""
        kv_store.set("expire_key", "expire_value", ttl=0.1)  # 100ms
        
        # Key should exist initially
        assert kv_store.exists("expire_key") is True
        
        # Wait for expiration
        time.sleep(0.2)
        
        # Key should be gone
        assert kv_store.exists("expire_key") is False
        with pytest.raises(KeyNotFoundError):
            kv_store.get("expire_key")

    def test_ttl_no_expiration(self, kv_store):
        """Test keys without TTL don't expire."""
        kv_store.set("persistent_key", "persistent_value")
        
        assert kv_store.ttl("persistent_key") is None
        
        # Wait a bit and verify key still exists
        time.sleep(0.1)
        assert kv_store.exists("persistent_key") is True

    def test_ttl_nonexistent_key(self, kv_store):
        """Test TTL for non-existent key."""
        assert kv_store.ttl("nonexistent_key") is None


class TestKVStoreDataTypes:
    """Test storing different data types."""

    def test_string_values(self, kv_store):
        """Test storing string values."""
        kv_store.set("string_key", "string_value")
        assert kv_store.get("string_key") == "string_value"

    def test_integer_values(self, kv_store):
        """Test storing integer values."""
        kv_store.set("int_key", 42)
        assert kv_store.get("int_key") == 42

    def test_float_values(self, kv_store):
        """Test storing float values."""
        kv_store.set("float_key", 3.14159)
        assert kv_store.get("float_key") == 3.14159

    def test_list_values(self, kv_store):
        """Test storing list values."""
        test_list = [1, 2, 3, "four", 5.0]
        kv_store.set("list_key", test_list)
        assert kv_store.get("list_key") == test_list

    def test_dict_values(self, kv_store):
        """Test storing dictionary values."""
        test_dict = {"name": "test", "value": 123, "nested": {"key": "value"}}
        kv_store.set("dict_key", test_dict)
        assert kv_store.get("dict_key") == test_dict

    def test_none_values(self, kv_store):
        """Test storing None values."""
        kv_store.set("none_key", None)
        assert kv_store.get("none_key") is None