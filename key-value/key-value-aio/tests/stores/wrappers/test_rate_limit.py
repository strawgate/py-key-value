import asyncio

import pytest
from key_value.shared.errors.wrappers.rate_limit import RateLimitExceededError
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.rate_limit import RateLimitWrapper
from tests.stores.base import BaseStoreTests


class TestRateLimitWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> RateLimitWrapper:
        return RateLimitWrapper(key_value=memory_store, max_requests=100, window_seconds=60.0)

    async def test_rate_limit_allows_requests_within_limit(self, memory_store: MemoryStore):
        rate_limiter = RateLimitWrapper(key_value=memory_store, max_requests=10, window_seconds=1.0)

        # Should allow up to 10 requests
        for i in range(10):
            await rate_limiter.put(collection="test", key=f"key{i}", value={"value": i})

        # Should not raise any errors
        assert True

    async def test_rate_limit_blocks_requests_exceeding_limit_sliding(self, memory_store: MemoryStore):
        rate_limiter = RateLimitWrapper(key_value=memory_store, max_requests=5, window_seconds=1.0, strategy="sliding")

        # Make 5 requests (at the limit)
        for i in range(5):
            await rate_limiter.put(collection="test", key=f"key{i}", value={"value": i})

        # 6th request should be blocked
        with pytest.raises(RateLimitExceededError) as exc_info:
            await rate_limiter.put(collection="test", key="key6", value={"value": 6})

        assert exc_info.value.extra_info is not None
        assert exc_info.value.extra_info["max_requests"] == 5
        assert exc_info.value.extra_info["current_requests"] == 5

    async def test_rate_limit_blocks_requests_exceeding_limit_fixed(self, memory_store: MemoryStore):
        rate_limiter = RateLimitWrapper(key_value=memory_store, max_requests=5, window_seconds=1.0, strategy="fixed")

        # Make 5 requests (at the limit)
        for i in range(5):
            await rate_limiter.put(collection="test", key=f"key{i}", value={"value": i})

        # 6th request should be blocked
        with pytest.raises(RateLimitExceededError):
            await rate_limiter.put(collection="test", key="key6", value={"value": 6})

    async def test_rate_limit_resets_after_window_sliding(self, memory_store: MemoryStore):
        rate_limiter = RateLimitWrapper(key_value=memory_store, max_requests=5, window_seconds=0.1, strategy="sliding")

        # Make 5 requests
        for i in range(5):
            await rate_limiter.put(collection="test", key=f"key{i}", value={"value": i})

        # Wait for window to expire
        await asyncio.sleep(0.15)

        # Should be able to make more requests
        await rate_limiter.put(collection="test", key="key6", value={"value": 6})
        assert True

    async def test_rate_limit_resets_after_window_fixed(self, memory_store: MemoryStore):
        rate_limiter = RateLimitWrapper(key_value=memory_store, max_requests=5, window_seconds=0.1, strategy="fixed")

        # Make 5 requests
        for i in range(5):
            await rate_limiter.put(collection="test", key=f"key{i}", value={"value": i})

        # Wait for window to expire
        await asyncio.sleep(0.15)

        # Should be able to make more requests
        await rate_limiter.put(collection="test", key="key6", value={"value": 6})
        assert True

    async def test_rate_limit_sliding_window_partial_reset(self, memory_store: MemoryStore):
        rate_limiter = RateLimitWrapper(key_value=memory_store, max_requests=5, window_seconds=0.2, strategy="sliding")

        # Make 5 requests
        for i in range(5):
            await rate_limiter.put(collection="test", key=f"key{i}", value={"value": i})

        # 6th request should fail
        with pytest.raises(RateLimitExceededError):
            await rate_limiter.put(collection="test", key="key6", value={"value": 6})

        # Wait for part of the window to expire (oldest request drops off)
        await asyncio.sleep(0.1)

        # Should be able to make one more request (one old request dropped off)
        await rate_limiter.put(collection="test", key="key6", value={"value": 6})

    async def test_rate_limit_applies_to_all_operations(self, memory_store: MemoryStore):
        rate_limiter = RateLimitWrapper(key_value=memory_store, max_requests=3, window_seconds=1.0)

        # Mix different operations
        await rate_limiter.put(collection="test", key="key1", value={"value": 1})
        await rate_limiter.get(collection="test", key="key1")
        await rate_limiter.delete(collection="test", key="key1")

        # 4th operation should be blocked
        with pytest.raises(RateLimitExceededError):
            await rate_limiter.put(collection="test", key="key2", value={"value": 2})

    async def test_rate_limit_concurrent_requests(self, memory_store: MemoryStore):
        rate_limiter = RateLimitWrapper(key_value=memory_store, max_requests=10, window_seconds=1.0)

        # Create 15 concurrent requests
        tasks = [rate_limiter.put(collection="test", key=f"key{i}", value={"value": i}) for i in range(15)]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Should have exactly 10 successes and 5 RateLimitExceededErrors
        successes = sum(1 for r in results if r is None)
        errors = sum(1 for r in results if isinstance(r, RateLimitExceededError))

        assert successes == 10
        assert errors == 5

    async def test_rate_limit_default_parameters(self, memory_store: MemoryStore):
        rate_limiter = RateLimitWrapper(key_value=memory_store)

        # Should use defaults: 100 requests per 60 seconds
        assert rate_limiter.max_requests == 100
        assert rate_limiter.window_seconds == 60.0
        assert rate_limiter.strategy == "sliding"
