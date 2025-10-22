import pytest
from cryptography.fernet import Fernet
from typing import Any
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.encryption import EncryptionWrapper
from tests.stores.base import BaseStoreTests


class TestEncryptionWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> EncryptionWrapper:
        # Generate a key for testing
        encryption_key = Fernet.generate_key()
        return EncryptionWrapper(key_value=memory_store, encryption_key=encryption_key)

    async def test_encryption_encrypts_value(self, memory_store: MemoryStore):
        """Test that values are actually encrypted in the underlying store."""
        encryption_key = Fernet.generate_key()
        encryption_store = EncryptionWrapper(key_value=memory_store, encryption_key=encryption_key)

        original_value = {"test": "value", "number": 123}
        await encryption_store.put(collection="test", key="test", value=original_value)

        # Check the underlying store - should be encrypted
        raw_value = await memory_store.get(collection="test", key="test")
        assert raw_value is not None
        assert "__encrypted_data__" in raw_value
        assert "__encryption_version__" in raw_value
        assert isinstance(raw_value["__encrypted_data__"], str)

        # The encrypted data should not contain the original value
        assert "test" not in str(raw_value)
        assert "value" not in str(raw_value)

        # Retrieve through wrapper - should decrypt automatically
        result = await encryption_store.get(collection="test", key="test")
        assert result == original_value

    async def test_encryption_with_string_key(self, memory_store: MemoryStore):
        """Test that encryption works with a string key."""
        encryption_key = Fernet.generate_key().decode("utf-8")
        encryption_store = EncryptionWrapper(key_value=memory_store, encryption_key=encryption_key)

        original_value = {"test": "value"}
        await encryption_store.put(collection="test", key="test", value=original_value)

        result = await encryption_store.get(collection="test", key="test")
        assert result == original_value

    async def test_encryption_many_operations(self, memory_store: MemoryStore):
        """Test that encryption works with put_many and get_many."""
        encryption_key = Fernet.generate_key()
        encryption_store = EncryptionWrapper(key_value=memory_store, encryption_key=encryption_key)

        keys = ["k1", "k2", "k3"]
        values = [{"data": "value1"}, {"data": "value2"}, {"data": "value3"}]

        await encryption_store.put_many(collection="test", keys=keys, values=values)

        # Check underlying store - all should be encrypted
        for key in keys:
            raw_value = await memory_store.get(collection="test", key=key)
            assert raw_value is not None
            assert "__encrypted_data__" in raw_value

        # Retrieve through wrapper
        results = await encryption_store.get_many(collection="test", keys=keys)
        assert results == values

    async def test_encryption_already_encrypted_not_reencrypted(self, memory_store: MemoryStore):
        """Test that already encrypted values are not re-encrypted."""
        encryption_key = Fernet.generate_key()
        encryption_store = EncryptionWrapper(key_value=memory_store, encryption_key=encryption_key)

        # Manually create an encrypted value
        encrypted_value = {
            "__encrypted_data__": "gAAAAABmxxx...",  # Mock encrypted data
            "__encryption_version__": 1,
        }

        # Should not try to encrypt again
        result = encryption_store._encrypt_value(value=encrypted_value)  # pyright: ignore[reportPrivateUsage]
        assert result == encrypted_value

    async def test_decryption_handles_unencrypted_data(self, memory_store: MemoryStore):
        """Test that unencrypted data is returned as-is."""
        encryption_key = Fernet.generate_key()
        encryption_store = EncryptionWrapper(key_value=memory_store, encryption_key=encryption_key)

        # Store unencrypted data directly in underlying store
        unencrypted_value = {"test": "value"}
        await memory_store.put(collection="test", key="test", value=unencrypted_value)

        # Should return as-is when retrieved through encryption wrapper
        result = await encryption_store.get(collection="test", key="test")
        assert result == unencrypted_value

    async def test_decryption_handles_corrupted_data(self, memory_store: MemoryStore):
        """Test that corrupted encrypted data is handled gracefully."""
        encryption_key = Fernet.generate_key()
        encryption_store = EncryptionWrapper(key_value=memory_store, encryption_key=encryption_key)

        # Store corrupted encrypted data
        corrupted_value = {
            "__encrypted_data__": "invalid-encrypted-data!!!",
            "__encryption_version__": 1,
        }
        await memory_store.put(collection="test", key="test", value=corrupted_value)

        # Should return the corrupted value as-is rather than crashing
        result = await encryption_store.get(collection="test", key="test")
        assert result == corrupted_value

    async def test_decryption_with_wrong_key_returns_original(self, memory_store: MemoryStore):
        """Test that decryption with the wrong key returns the original encrypted value."""
        encryption_key1 = Fernet.generate_key()
        encryption_key2 = Fernet.generate_key()

        encryption_store1 = EncryptionWrapper(key_value=memory_store, encryption_key=encryption_key1)
        encryption_store2 = EncryptionWrapper(key_value=memory_store, encryption_key=encryption_key2)

        original_value = {"test": "value"}
        await encryption_store1.put(collection="test", key="test", value=original_value)

        # Try to retrieve with a different key - should return the encrypted data as-is
        result = await encryption_store2.get(collection="test", key="test")
        # Since decryption fails, it should return the encrypted dict
        assert result is not None
        assert "__encrypted_data__" in result

    async def test_encryption_with_ttl(self, memory_store: MemoryStore):
        """Test that encryption works with TTL."""
        encryption_key = Fernet.generate_key()
        encryption_store = EncryptionWrapper(key_value=memory_store, encryption_key=encryption_key)

        original_value = {"test": "value"}
        await encryption_store.put(collection="test", key="test", value=original_value, ttl=3600)

        # Check underlying store - should be encrypted
        raw_value = await memory_store.get(collection="test", key="test")
        assert raw_value is not None
        assert "__encrypted_data__" in raw_value

        # Retrieve through wrapper with TTL
        result, ttl = await encryption_store.ttl(collection="test", key="test")
        assert result == original_value
        assert ttl is not None
        assert ttl > 0

    async def test_encryption_ttl_many(self, memory_store: MemoryStore):
        """Test that encryption works with ttl_many."""
        encryption_key = Fernet.generate_key()
        encryption_store = EncryptionWrapper(key_value=memory_store, encryption_key=encryption_key)

        keys = ["k1", "k2"]
        values = [{"data": "value1"}, {"data": "value2"}]
        ttls = [3600, 7200]

        await encryption_store.put_many(collection="test", keys=keys, values=values, ttl=ttls)

        # Retrieve through wrapper with TTL
        results = await encryption_store.ttl_many(collection="test", keys=keys)
        assert len(results) == 2
        for (value, ttl), expected_value in zip(results, values, strict=True):
            assert value == expected_value
            assert ttl is not None
            assert ttl > 0

    async def test_encryption_complex_data(self, memory_store: MemoryStore):
        """Test encryption with complex nested data structures."""
        encryption_key = Fernet.generate_key()
        encryption_store = EncryptionWrapper(key_value=memory_store, encryption_key=encryption_key)

        complex_value = {
            "users": [
                {"name": "Alice", "age": 30, "active": True},
                {"name": "Bob", "age": 25, "active": False},
            ],
            "metadata": {
                "created_at": "2024-01-01T00:00:00Z",
                "version": 1,
            },
            "tags": ["python", "encryption", "testing"],
        }

        await encryption_store.put(collection="test", key="complex", value=complex_value)

        # Check underlying store - should be encrypted
        raw_value = await memory_store.get(collection="test", key="complex")
        assert raw_value is not None
        assert "__encrypted_data__" in raw_value

        # Retrieve and verify
        result = await encryption_store.get(collection="test", key="complex")
        assert result == complex_value

    async def test_encryption_empty_dict(self, memory_store: MemoryStore):
        """Test encryption with an empty dictionary."""
        encryption_key = Fernet.generate_key()
        encryption_store = EncryptionWrapper(key_value=memory_store, encryption_key=encryption_key)

        empty_value: dict[str, Any] = {}
        await encryption_store.put(collection="test", key="empty", value=empty_value)

        result = await encryption_store.get(collection="test", key="empty")
        assert result == empty_value
