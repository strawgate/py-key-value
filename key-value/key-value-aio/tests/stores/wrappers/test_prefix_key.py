import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.prefix_keys import PrefixKeysWrapper
from tests.stores.base import BaseStoreTests


class TestPrefixKeyWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> PrefixKeysWrapper:
        return PrefixKeysWrapper(key_value=memory_store, prefix="key_prefix")

    async def test_prefix_isolation(self, memory_store: MemoryStore) -> None:
        """Test that different prefixes create isolated key spaces."""
        wrapper1 = PrefixKeysWrapper(key_value=memory_store, prefix="prefix1")
        wrapper2 = PrefixKeysWrapper(key_value=memory_store, prefix="prefix2")

        # Put same key in both wrappers
        await wrapper1.put(collection="test", key="key", value={"source": "wrapper1"})
        await wrapper2.put(collection="test", key="key", value={"source": "wrapper2"})

        # Each wrapper should see its own value
        result1 = await wrapper1.get(collection="test", key="key")
        result2 = await wrapper2.get(collection="test", key="key")
        assert result1 == {"source": "wrapper1"}
        assert result2 == {"source": "wrapper2"}

        # After deleting from wrapper1, wrapper2 should still have its value
        await wrapper1.delete(collection="test", key="key")
        assert await wrapper1.get(collection="test", key="key") is None
        assert await wrapper2.get(collection="test", key="key") == {"source": "wrapper2"}

    async def test_prefix_get_many(self, memory_store: MemoryStore) -> None:
        """Test that get_many applies prefix to all keys."""
        wrapper = PrefixKeysWrapper(key_value=memory_store, prefix="pre")

        # Put multiple values through wrapper
        await wrapper.put_many(collection="test", keys=["k1", "k2", "k3"], values=[{"v": 1}, {"v": 2}, {"v": 3}])

        # Get many should work correctly
        results = await wrapper.get_many(collection="test", keys=["k1", "k2", "k3"])
        assert results == [{"v": 1}, {"v": 2}, {"v": 3}]

        # Get many with missing keys should still work
        results_with_missing = await wrapper.get_many(collection="test", keys=["k1", "missing", "k3"])
        assert results_with_missing[0] == {"v": 1}
        assert results_with_missing[1] is None
        assert results_with_missing[2] == {"v": 3}

    async def test_prefix_put_many(self, memory_store: MemoryStore) -> None:
        """Test that put_many applies prefix to all keys."""
        wrapper = PrefixKeysWrapper(key_value=memory_store, prefix="bulk")

        # Put many through wrapper
        await wrapper.put_many(collection="test", keys=["a", "b", "c"], values=[{"v": "a"}, {"v": "b"}, {"v": "c"}])

        # Verify we can retrieve all values
        result_a = await wrapper.get(collection="test", key="a")
        result_b = await wrapper.get(collection="test", key="b")
        result_c = await wrapper.get(collection="test", key="c")
        assert result_a == {"v": "a"}
        assert result_b == {"v": "b"}
        assert result_c == {"v": "c"}

    async def test_prefix_delete_isolation(self, memory_store: MemoryStore) -> None:
        """Test that delete only affects prefixed keys."""
        wrapper1 = PrefixKeysWrapper(key_value=memory_store, prefix="p1")
        wrapper2 = PrefixKeysWrapper(key_value=memory_store, prefix="p2")

        # Put same key through both wrappers
        await wrapper1.put(collection="test", key="key", value={"source": "wrapper1"})
        await wrapper2.put(collection="test", key="key", value={"source": "wrapper2"})

        # Delete through wrapper1 should only affect wrapper1's key
        deleted = await wrapper1.delete(collection="test", key="key")
        assert deleted is True

        # wrapper1 should not have the key
        result1 = await wrapper1.get(collection="test", key="key")
        assert result1 is None

        # wrapper2 should still have its key
        result2 = await wrapper2.get(collection="test", key="key")
        assert result2 == {"source": "wrapper2"}

    async def test_prefix_delete_many(self, memory_store: MemoryStore) -> None:
        """Test that delete_many applies prefix to all keys."""
        wrapper = PrefixKeysWrapper(key_value=memory_store, prefix="d")

        # Put multiple values
        await wrapper.put_many(collection="test", keys=["k1", "k2", "k3"], values=[{"v": 1}, {"v": 2}, {"v": 3}])

        # Delete many
        deleted = await wrapper.delete_many(collection="test", keys=["k1", "k2"])
        assert deleted == 2

        # k1 and k2 should be gone, k3 should remain
        result_k1 = await wrapper.get(collection="test", key="k1")
        result_k2 = await wrapper.get(collection="test", key="k2")
        result_k3 = await wrapper.get(collection="test", key="k3")
        assert result_k1 is None
        assert result_k2 is None
        assert result_k3 == {"v": 3}

    async def test_prefix_ttl(self, memory_store: MemoryStore) -> None:
        """Test that ttl operation applies prefix correctly."""
        wrapper = PrefixKeysWrapper(key_value=memory_store, prefix="ttl")

        # Put value with TTL through wrapper
        await wrapper.put(collection="test", key="key", value={"data": "value"}, ttl=100)

        # Query ttl through wrapper
        result, ttl = await wrapper.ttl(collection="test", key="key")
        assert result == {"data": "value"}
        assert ttl is not None
        assert ttl <= 100

    async def test_prefix_ttl_many(self, memory_store: MemoryStore) -> None:
        """Test that ttl_many applies prefix to all keys."""
        wrapper = PrefixKeysWrapper(key_value=memory_store, prefix="multi_ttl")

        # Put values with TTL
        await wrapper.put_many(collection="test", keys=["k1", "k2"], values=[{"v": 1}, {"v": 2}], ttl=60)

        # Query ttl for multiple keys
        results = await wrapper.ttl_many(collection="test", keys=["k1", "k2", "k3"])
        assert results[0][0] == {"v": 1}
        assert results[1][0] == {"v": 2}
        assert results[2][0] is None

    async def test_prefix_with_special_characters(self, memory_store: MemoryStore) -> None:
        """Test that prefix handles special characters in keys."""
        wrapper = PrefixKeysWrapper(key_value=memory_store, prefix="special")

        # Put with special character keys
        await wrapper.put(collection="test", key="user:123", value={"user_id": 123})
        await wrapper.put(collection="test", key="data@host", value={"host": "example.com"})

        # Get should work correctly
        result1 = await wrapper.get(collection="test", key="user:123")
        result2 = await wrapper.get(collection="test", key="data@host")
        assert result1 == {"user_id": 123}
        assert result2 == {"host": "example.com"}

    async def test_prefix_empty_string_keys(self, memory_store: MemoryStore) -> None:
        """Test that prefix handles empty string operations correctly."""
        wrapper = PrefixKeysWrapper(key_value=memory_store, prefix="empty")

        # Put empty value dict
        await wrapper.put(collection="test", key="key", value={})

        # Get should return empty dict
        result = await wrapper.get(collection="test", key="key")
        assert result == {}
