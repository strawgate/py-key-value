import pytest
from cryptography.fernet import Fernet
from dirty_equals import IsStr
from inline_snapshot import snapshot
from key_value.shared.errors.wrappers.encryption import DecryptionError
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.encryption import FernetEncryptionWrapper
from key_value.aio.wrappers.encryption.fernet import _generate_encryption_key  # pyright: ignore[reportPrivateUsage]
from tests.stores.base import BaseStoreTests


@pytest.fixture
def fernet() -> Fernet:
    return Fernet(key=Fernet.generate_key())


class TestFernetEncryptionWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore, fernet: Fernet) -> FernetEncryptionWrapper:
        return FernetEncryptionWrapper(key_value=memory_store, fernet=fernet)

    async def test_encryption_encrypts_value(self, store: FernetEncryptionWrapper, memory_store: MemoryStore):
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

    async def test_encryption_with_wrong_encryption_version(self, store: FernetEncryptionWrapper):
        """Test that encryption fails with the wrong encryption version."""
        store.encryption_version = 2
        original_value = {"test": "value"}
        await store.put(collection="test", key="test", value=original_value)

        assert await store.get(collection="test", key="test") is not None
        store.encryption_version = 1

        with pytest.raises(DecryptionError):
            await store.get(collection="test", key="test")

    async def test_encryption_with_string_key(self, store: FernetEncryptionWrapper, memory_store: MemoryStore):
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

    async def test_encryption_many_operations(self, store: FernetEncryptionWrapper, memory_store: MemoryStore):
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

    async def test_decryption_handles_unencrypted_data(self, store: FernetEncryptionWrapper, memory_store: MemoryStore):
        """Test that unencrypted data is returned as-is."""
        # Store unencrypted data directly in underlying store
        unencrypted_value = {"test": "value"}
        await memory_store.put(collection="test", key="test", value=unencrypted_value)

        # Should return as-is when retrieved through encryption wrapper
        result = await store.get(collection="test", key="test")
        assert result == unencrypted_value

    async def test_decryption_handles_corrupted_data(self, store: FernetEncryptionWrapper, memory_store: MemoryStore):
        """Test that corrupted encrypted data is handled gracefully."""

        # Store corrupted encrypted data
        corrupted_value = {
            "__encrypted_data__": "invalid-encrypted-data!!!",
            "__encryption_version__": 1,
        }
        await memory_store.put(collection="test", key="test", value=corrupted_value)

        with pytest.raises(DecryptionError):
            await store.get(collection="test", key="test")

    async def test_decryption_ignores_corrupted_data(self, memory_store: MemoryStore, fernet: Fernet):
        """Test that corrupted encrypted data is ignored."""
        store = FernetEncryptionWrapper(key_value=memory_store, fernet=fernet, raise_on_decryption_error=False)

        # Store corrupted encrypted data
        corrupted_value = {
            "__encrypted_data__": "invalid-encrypted-data!!!",
            "__encryption_version__": 1,
        }
        await memory_store.put(collection="test", key="test", value=corrupted_value)

        assert await store.get(collection="test", key="test") is None

    async def test_decryption_with_wrong_key_raises_error(self, memory_store: MemoryStore):
        """Test that decryption with the wrong key raises an error."""
        fernet1 = Fernet(key=Fernet.generate_key())
        fernet2 = Fernet(key=Fernet.generate_key())

        store1 = FernetEncryptionWrapper(key_value=memory_store, fernet=fernet1)
        store2 = FernetEncryptionWrapper(key_value=memory_store, fernet=fernet2)

        original_value = {"test": "value"}
        await store1.put(collection="test", key="test", value=original_value)

        with pytest.raises(DecryptionError):
            await store2.get(collection="test", key="test")


def test_key_generation():
    """Test that key generation works with a source material and salt and that different source materials and salts produce different keys."""

    source_material = "test-source-material"
    salt = "test-salt"
    key = _generate_encryption_key(source_material=source_material, salt=salt)
    key_str_one = key.decode()
    assert key_str_one == snapshot("znx7rVYt4roVgu3ymt5sIYFmfMNGEPbm8AShXQv6CY4=")

    source_material = "different-source-material"
    salt = "test-salt"
    key = _generate_encryption_key(source_material=source_material, salt=salt)
    key_str_two = key.decode()
    assert key_str_two == snapshot("1TLRpjxQm4Op699i9hAXFVfyz6PqPXbuvwKaWB48tS8=")

    source_material = "test-source-material"
    salt = "different-salt"
    key = _generate_encryption_key(source_material=source_material, salt=salt)
    key_str_three = key.decode()
    assert key_str_three == snapshot("oLz_g5NoLhANNh2_-ZwbgchDZ1q23VFx90kUQDjracc=")

    assert key_str_one != key_str_two
    assert key_str_one != key_str_three
    assert key_str_two != key_str_three
