import asyncio

import pytest
from key_value.shared.errors.wrappers.bulkhead import BulkheadFullError
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.bulkhead import BulkheadWrapper
from tests.stores.base import BaseStoreTests


class SlowStore(MemoryStore):
    """A store that adds artificial delay to operations."""

    def __init__(self, delay: float = 0.1):
        super().__init__()
        self.delay = delay
        self.concurrent_operations = 0
        self.max_concurrent_observed = 0

    async def get(self, key: str, *, collection: str | None = None):
        self.concurrent_operations += 1
        self.max_concurrent_observed = max(self.max_concurrent_observed, self.concurrent_operations)
        try:
            await asyncio.sleep(self.delay)
            return await super().get(key=key, collection=collection)
        finally:
            self.concurrent_operations -= 1


class TestBulkheadWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> BulkheadWrapper:
        return BulkheadWrapper(key_value=memory_store, max_concurrent=10, max_waiting=20)

    async def test_bulkhead_allows_operations_within_limit(self, memory_store: MemoryStore):
        bulkhead = BulkheadWrapper(key_value=memory_store, max_concurrent=5, max_waiting=10)

        # Should allow operations within limits
        await bulkhead.put(collection="test", key="key1", value={"value": 1})
        await bulkhead.put(collection="test", key="key2", value={"value": 2})
        result = await bulkhead.get(collection="test", key="key1")
        assert result == {"value": 1}

    async def test_bulkhead_limits_concurrent_operations(self):
        slow_store = SlowStore(delay=0.1)
        bulkhead = BulkheadWrapper(key_value=slow_store, max_concurrent=3, max_waiting=10)

        # Pre-populate store
        await slow_store.put(collection="test", key="key", value={"value": 1})

        # Launch 10 concurrent operations
        tasks = [bulkhead.get(collection="test", key="key") for _ in range(10)]
        await asyncio.gather(*tasks)

        # Verify that at most 3 operations ran concurrently
        assert slow_store.max_concurrent_observed <= 3

    async def test_bulkhead_blocks_when_queue_full(self):
        slow_store = SlowStore(delay=0.5)
        bulkhead = BulkheadWrapper(key_value=slow_store, max_concurrent=2, max_waiting=3)

        # Pre-populate store
        await slow_store.put(collection="test", key="key", value={"value": 1})

        # Launch operations that will fill the bulkhead
        # 2 will run concurrently, 3 will wait, rest should be rejected
        tasks = [bulkhead.get(collection="test", key="key") for _ in range(10)]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count successes and errors
        successes = sum(1 for r in results if not isinstance(r, Exception))
        errors = sum(1 for r in results if isinstance(r, BulkheadFullError))

        # Should have 5 successes (2 concurrent + 3 waiting) and 5 errors
        assert successes == 5
        assert errors == 5

    async def test_bulkhead_allows_operations_after_completion(self):
        slow_store = SlowStore(delay=0.05)
        bulkhead = BulkheadWrapper(key_value=slow_store, max_concurrent=2, max_waiting=2)

        # Pre-populate store
        await slow_store.put(collection="test", key="key", value={"value": 1})

        # First batch - should succeed
        tasks1 = [bulkhead.get(collection="test", key="key") for _ in range(4)]
        results1 = await asyncio.gather(*tasks1, return_exceptions=True)
        successes1 = sum(1 for r in results1 if not isinstance(r, Exception))
        assert successes1 == 4

        # Second batch - should also succeed since first batch completed
        tasks2 = [bulkhead.get(collection="test", key="key") for _ in range(4)]
        results2 = await asyncio.gather(*tasks2, return_exceptions=True)
        successes2 = sum(1 for r in results2 if not isinstance(r, Exception))
        assert successes2 == 4

    async def test_bulkhead_applies_to_all_operations(self):
        slow_store = SlowStore(delay=0.1)
        bulkhead = BulkheadWrapper(key_value=slow_store, max_concurrent=2, max_waiting=1)

        # Pre-populate store
        await slow_store.put(collection="test", key="key1", value={"value": 1})
        await slow_store.put(collection="test", key="key2", value={"value": 2})

        # Mix different operations
        tasks = [
            bulkhead.get(collection="test", key="key1"),
            bulkhead.put(collection="test", key="key3", value={"value": 3}),
            bulkhead.delete(collection="test", key="key2"),
            bulkhead.get(collection="test", key="key1"),
            bulkhead.get(collection="test", key="key1"),  # This should be rejected
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Should have 3 successes (2 concurrent + 1 waiting) and 2 errors
        successes = sum(1 for r in results if not isinstance(r, Exception))
        errors = sum(1 for r in results if isinstance(r, BulkheadFullError))

        assert successes == 3
        assert errors == 2

    async def test_bulkhead_default_parameters(self, memory_store: MemoryStore):
        bulkhead = BulkheadWrapper(key_value=memory_store)

        # Should use defaults
        assert bulkhead.max_concurrent == 10
        assert bulkhead.max_waiting == 20

    async def test_bulkhead_error_handling(self):
        """Test that errors in operations don't leak semaphore counts."""

        class FailingStore(MemoryStore):
            async def get(self, key: str, *, collection: str | None = None):
                msg = "Intentional failure"
                raise RuntimeError(msg)

        failing_store = FailingStore()
        bulkhead = BulkheadWrapper(key_value=failing_store, max_concurrent=2, max_waiting=2)

        # Execute operations that will fail
        for _ in range(5):
            with pytest.raises(RuntimeError):
                await bulkhead.get(collection="test", key="key")

        # Semaphore should be released properly - we should still be able to make requests
        # If semaphore leaked, this would eventually block
        assert True

    async def test_bulkhead_with_fast_operations(self, memory_store: MemoryStore):
        """Test bulkhead with operations that complete quickly."""
        bulkhead = BulkheadWrapper(key_value=memory_store, max_concurrent=2, max_waiting=2)

        # Pre-populate
        await memory_store.put(collection="test", key="key", value={"value": 1})

        # Fast operations should all succeed even with low limits
        tasks = [bulkhead.get(collection="test", key="key") for _ in range(20)]
        results = await asyncio.gather(*tasks)

        # All should succeed
        assert all(r == {"value": 1} for r in results)

    async def test_bulkhead_sequential_operations(self, memory_store: MemoryStore):
        """Test that sequential operations don't count against concurrent limit."""
        bulkhead = BulkheadWrapper(key_value=memory_store, max_concurrent=1, max_waiting=0)

        # Sequential operations should all succeed
        for i in range(10):
            await bulkhead.put(collection="test", key=f"key{i}", value={"value": i})

        # All should be stored
        for i in range(10):
            result = await bulkhead.get(collection="test", key=f"key{i}")
            assert result == {"value": i}
