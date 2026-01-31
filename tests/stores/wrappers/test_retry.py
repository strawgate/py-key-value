import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.retry import RetryWrapper
from tests.stores.base import BaseStoreTests


class FailingStore(MemoryStore):
    """A store that fails a certain number of times before succeeding."""

    def __init__(self, failures_before_success: int = 2):
        super().__init__()
        self.failures_before_success = failures_before_success
        self.attempt_count = 0

    async def get(self, key: str, *, collection: str | None = None):
        self.attempt_count += 1
        if self.attempt_count <= self.failures_before_success:
            msg = "Simulated connection error"
            raise ConnectionError(msg)
        return await super().get(key=key, collection=collection)

    def reset_attempts(self):
        self.attempt_count = 0


class TestRetryWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> RetryWrapper:
        return RetryWrapper(key_value=memory_store, max_retries=3, initial_delay=0.01)

    async def test_retry_succeeds_after_failures(self):
        failing_store = FailingStore(failures_before_success=2)
        retry_store = RetryWrapper(key_value=failing_store, max_retries=3, initial_delay=0.01)

        # Store a value first
        await retry_store.put(collection="test", key="test", value={"test": "value"})
        failing_store.reset_attempts()

        # Should succeed after 2 failures
        result = await retry_store.get(collection="test", key="test")
        assert result == {"test": "value"}
        assert failing_store.attempt_count == 3  # 2 failures + 1 success

    async def test_retry_fails_after_max_retries(self):
        failing_store = FailingStore(failures_before_success=10)  # More failures than max_retries
        retry_store = RetryWrapper(key_value=failing_store, max_retries=2, initial_delay=0.01)

        # Should fail after exhausting retries
        with pytest.raises(ConnectionError):
            await retry_store.get(collection="test", key="test")

        assert failing_store.attempt_count == 3  # Initial attempt + 2 retries

    async def test_retry_with_different_exception(self):
        failing_store = FailingStore(failures_before_success=1)
        # Only retry on TimeoutError, not ConnectionError
        retry_store = RetryWrapper(key_value=failing_store, max_retries=3, initial_delay=0.01, retry_on=(TimeoutError,))

        # Should fail immediately without retries
        with pytest.raises(ConnectionError):
            await retry_store.get(collection="test", key="test")

        assert failing_store.attempt_count == 1  # No retries

    async def test_retry_no_failures(self, memory_store: MemoryStore):
        retry_store = RetryWrapper(key_value=memory_store, max_retries=3, initial_delay=0.01)

        # Normal operation should work without retries
        await retry_store.put(collection="test", key="test", value={"test": "value"})
        result = await retry_store.get(collection="test", key="test")
        assert result == {"test": "value"}
