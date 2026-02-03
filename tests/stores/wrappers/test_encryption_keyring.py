from unittest.mock import patch

import pytest
from cryptography.fernet import Fernet
from dirty_equals import IsStr
from inline_snapshot import snapshot
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.encryption.keyring import (
    DEFAULT_KEY_NAME,
    DEFAULT_SERVICE_NAME,
    KeyringEncryptionWrapper,
)
from key_value.shared.errors import DecryptionError
from tests.stores.base import BaseStoreTests


class MockKeyring:
    """A simple mock keyring that stores passwords in memory."""

    def __init__(self) -> None:
        self._storage: dict[tuple[str, str], str] = {}

    def get_password(self, service_name: str, username: str) -> str | None:
        return self._storage.get((service_name, username))

    def set_password(self, service_name: str, username: str, password: str) -> None:
        self._storage[(service_name, username)] = password

    def delete_password(self, service_name: str, username: str) -> None:
        del self._storage[(service_name, username)]


@pytest.fixture
def mock_keyring() -> MockKeyring:
    return MockKeyring()


@pytest.fixture
def patched_keyring(mock_keyring: MockKeyring):
    """Patch the keyring module in the encryption wrapper."""
    with patch("key_value.aio.wrappers.encryption.keyring.keyring", mock_keyring):
        yield mock_keyring


class TestKeyringEncryptionWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore, patched_keyring: MockKeyring) -> KeyringEncryptionWrapper:
        return KeyringEncryptionWrapper(key_value=memory_store)

    async def test_encryption_encrypts_value(
        self, store: KeyringEncryptionWrapper, memory_store: MemoryStore, patched_keyring: MockKeyring
    ):
        """Test that values are actually encrypted in the underlying store."""
        original_value = {"test": "value", "number": 123}
        await store.put(collection="test", key="test", value=original_value)

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
        result = await store.get(collection="test", key="test")
        assert result == original_value

    async def test_key_generation_on_first_use(self, memory_store: MemoryStore, patched_keyring: MockKeyring):
        """Test that a key is generated on first use when none exists."""
        # Ensure no key exists
        assert patched_keyring.get_password(service_name=DEFAULT_SERVICE_NAME, username=DEFAULT_KEY_NAME) is None

        # Create wrapper - should generate key
        KeyringEncryptionWrapper(key_value=memory_store)

        # Key should now exist
        stored_key = patched_keyring.get_password(service_name=DEFAULT_SERVICE_NAME, username=DEFAULT_KEY_NAME)
        assert stored_key is not None
        # Verify it's a valid Fernet key
        Fernet(stored_key.encode("ascii"))

    async def test_key_retrieval_on_subsequent_use(self, memory_store: MemoryStore, patched_keyring: MockKeyring):
        """Test that the same key is retrieved on subsequent instantiations."""
        # Create first wrapper
        wrapper1 = KeyringEncryptionWrapper(key_value=memory_store)
        original_value = {"test": "value"}
        await wrapper1.put(collection="test", key="test", value=original_value)

        # Create second wrapper - should use same key
        wrapper2 = KeyringEncryptionWrapper(key_value=memory_store)

        # Second wrapper should be able to decrypt data from first
        result = await wrapper2.get(collection="test", key="test")
        assert result == original_value

    async def test_custom_service_name_and_key_name(self, memory_store: MemoryStore, patched_keyring: MockKeyring):
        """Test that custom service name and key name are used."""
        custom_service = "my-custom-service"
        custom_key = "my-custom-key"

        KeyringEncryptionWrapper(key_value=memory_store, service_name=custom_service, key_name=custom_key)

        # Key should be stored under custom names
        assert patched_keyring.get_password(service_name=custom_service, username=custom_key) is not None
        # Default names should not have a key
        assert patched_keyring.get_password(service_name=DEFAULT_SERVICE_NAME, username=DEFAULT_KEY_NAME) is None

    async def test_encryption_with_string_key(
        self, store: KeyringEncryptionWrapper, memory_store: MemoryStore, patched_keyring: MockKeyring
    ):
        """Test that encryption works with a string key."""
        original_value = {"test": "value"}
        await store.put(collection="test", key="test", value=original_value)

        round_trip_value = await store.get(collection="test", key="test")
        assert round_trip_value == original_value

        raw_result = await memory_store.get(collection="test", key="test")
        assert raw_result == snapshot(
            {
                "__encrypted_data__": IsStr(min_length=32),
                "__encryption_version__": 1,
            }
        )

    async def test_encryption_many_operations(
        self, store: KeyringEncryptionWrapper, memory_store: MemoryStore, patched_keyring: MockKeyring
    ):
        """Test that encryption works with put_many and get_many."""
        keys = ["k1", "k2", "k3"]
        values = [{"data": "value1"}, {"data": "value2"}, {"data": "value3"}]

        await store.put_many(collection="test", keys=keys, values=values)

        # Check underlying store - all should be encrypted
        for key in keys:
            raw_value = await memory_store.get(collection="test", key=key)
            assert raw_value is not None
            assert "__encrypted_data__" in raw_value

        # Retrieve through wrapper
        results = await store.get_many(collection="test", keys=keys)
        assert results == values

    async def test_decryption_handles_unencrypted_data(
        self, store: KeyringEncryptionWrapper, memory_store: MemoryStore, patched_keyring: MockKeyring
    ):
        """Test that unencrypted data is returned as-is."""
        # Store unencrypted data directly in underlying store
        unencrypted_value = {"test": "value"}
        await memory_store.put(collection="test", key="test", value=unencrypted_value)

        # Should return as-is when retrieved through encryption wrapper
        result = await store.get(collection="test", key="test")
        assert result == unencrypted_value

    async def test_decryption_handles_corrupted_data(
        self, store: KeyringEncryptionWrapper, memory_store: MemoryStore, patched_keyring: MockKeyring
    ):
        """Test that corrupted encrypted data is handled gracefully."""
        # Store corrupted encrypted data
        corrupted_value = {
            "__encrypted_data__": "invalid-encrypted-data!!!",
            "__encryption_version__": 1,
        }
        await memory_store.put(collection="test", key="test", value=corrupted_value)

        with pytest.raises(DecryptionError):
            await store.get(collection="test", key="test")

    async def test_decryption_ignores_corrupted_data(self, memory_store: MemoryStore, patched_keyring: MockKeyring):
        """Test that corrupted encrypted data is ignored when configured."""
        store = KeyringEncryptionWrapper(key_value=memory_store, raise_on_decryption_error=False)

        # Store corrupted encrypted data
        corrupted_value = {
            "__encrypted_data__": "invalid-encrypted-data!!!",
            "__encryption_version__": 1,
        }
        await memory_store.put(collection="test", key="test", value=corrupted_value)

        assert await store.get(collection="test", key="test") is None

    async def test_key_rotation_with_old_keys(self, memory_store: MemoryStore, patched_keyring: MockKeyring):
        """Test that key rotation works with old_keys parameter."""
        # Create first wrapper and store data
        wrapper1 = KeyringEncryptionWrapper(key_value=memory_store, key_name="old-key")
        original_value = {"test": "value"}
        await wrapper1.put(collection="test", key="test", value=original_value)

        # Get the old key
        old_key = patched_keyring.get_password(service_name=DEFAULT_SERVICE_NAME, username="old-key")
        assert old_key is not None

        # Create new wrapper with different key name but providing old key for rotation
        wrapper2 = KeyringEncryptionWrapper(key_value=memory_store, key_name="new-key", old_keys=[old_key.encode("ascii")])

        # Should be able to decrypt data encrypted with old key
        result = await wrapper2.get(collection="test", key="test")
        assert result == original_value

    async def test_decryption_with_wrong_key_raises_error(self, memory_store: MemoryStore, patched_keyring: MockKeyring):
        """Test that decryption with the wrong key raises an error."""
        # Create two wrappers with different keys
        wrapper1 = KeyringEncryptionWrapper(key_value=memory_store, key_name="key1")
        wrapper2 = KeyringEncryptionWrapper(key_value=memory_store, key_name="key2")

        original_value = {"test": "value"}
        await wrapper1.put(collection="test", key="test", value=original_value)

        with pytest.raises(DecryptionError):
            await wrapper2.get(collection="test", key="test")

    async def test_different_service_names_are_isolated(self, memory_store: MemoryStore, patched_keyring: MockKeyring):
        """Test that different service names produce different keys and are isolated."""
        wrapper1 = KeyringEncryptionWrapper(key_value=memory_store, service_name="service1")
        wrapper2 = KeyringEncryptionWrapper(key_value=memory_store, service_name="service2")

        original_value = {"test": "value"}
        await wrapper1.put(collection="test", key="test", value=original_value)

        # Different service name means different key, should fail to decrypt
        with pytest.raises(DecryptionError):
            await wrapper2.get(collection="test", key="test")
