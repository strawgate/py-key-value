from typing import Any

import pytest
from key_value.shared.errors.wrappers.circuit_breaker import CircuitOpenError
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.circuit_breaker import CircuitBreakerWrapper
from key_value.aio.wrappers.circuit_breaker.wrapper import CircuitState
from tests.stores.base import BaseStoreTests


class IntermittentlyFailingStore(MemoryStore):
    """A store that fails a configurable number of times before succeeding."""

    def __init__(self, failures_before_success: int = 5):
        super().__init__()
        self.failures_before_success = failures_before_success
        self.attempt_count = 0

    def _check_and_maybe_fail(self):
        """Check if we should fail this operation."""
        self.attempt_count += 1
        if self.attempt_count <= self.failures_before_success:
            msg = "Simulated connection error"
            raise ConnectionError(msg)

    async def get(self, key: str, *, collection: str | None = None):
        self._check_and_maybe_fail()
        return await super().get(key=key, collection=collection)

    async def put(self, key: str, value: dict[str, Any], *, collection: str | None = None, ttl: float | None = None):
        self._check_and_maybe_fail()
        return await super().put(key=key, value=value, collection=collection, ttl=ttl)

    async def delete(self, key: str, *, collection: str | None = None):
        self._check_and_maybe_fail()
        return await super().delete(key=key, collection=collection)

    def reset_attempts(self):
        self.attempt_count = 0


class TestCircuitBreakerWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> CircuitBreakerWrapper:
        return CircuitBreakerWrapper(key_value=memory_store, failure_threshold=5, recovery_timeout=1.0)

    async def test_circuit_remains_closed_on_success(self, memory_store: MemoryStore):
        circuit_breaker = CircuitBreakerWrapper(key_value=memory_store, failure_threshold=3)

        # Successful operations should keep circuit closed
        await circuit_breaker.put(collection="test", key="test1", value={"test": "value1"})
        await circuit_breaker.put(collection="test", key="test2", value={"test": "value2"})
        await circuit_breaker.get(collection="test", key="test1")

        assert circuit_breaker._state == CircuitState.CLOSED  # pyright: ignore[reportPrivateUsage]
        assert circuit_breaker._failure_count == 0  # pyright: ignore[reportPrivateUsage]

    async def test_circuit_opens_after_threshold_failures(self):
        failing_store = IntermittentlyFailingStore(failures_before_success=10)
        circuit_breaker = CircuitBreakerWrapper(
            key_value=failing_store, failure_threshold=3, recovery_timeout=1.0, error_types=(ConnectionError,)
        )

        # First 3 failures should open the circuit
        for _ in range(3):
            with pytest.raises(ConnectionError):
                await circuit_breaker.get(collection="test", key="test")

        assert circuit_breaker._state == CircuitState.OPEN  # pyright: ignore[reportPrivateUsage]
        assert circuit_breaker._failure_count == 3  # pyright: ignore[reportPrivateUsage]

        # Next attempt should fail immediately with CircuitOpenError
        with pytest.raises(CircuitOpenError):
            await circuit_breaker.get(collection="test", key="test")

        # Verify we didn't make another attempt to the backend
        assert failing_store.attempt_count == 3

    async def test_circuit_transitions_to_half_open(self):
        failing_store = IntermittentlyFailingStore(failures_before_success=10)
        circuit_breaker = CircuitBreakerWrapper(
            key_value=failing_store, failure_threshold=3, recovery_timeout=0.1, error_types=(ConnectionError,)
        )

        # Open the circuit
        for _ in range(3):
            with pytest.raises(ConnectionError):
                await circuit_breaker.get(collection="test", key="test")

        assert circuit_breaker._state == CircuitState.OPEN  # pyright: ignore[reportPrivateUsage]

        # Wait for recovery timeout
        import asyncio

        await asyncio.sleep(0.15)

        # Next attempt should transition to half-open and try the operation
        with pytest.raises(ConnectionError):
            await circuit_breaker.get(collection="test", key="test")

        # Should be back to open since it failed
        assert circuit_breaker._state == CircuitState.OPEN  # pyright: ignore[reportPrivateUsage]

    async def test_circuit_closes_after_successful_recovery(self, memory_store: MemoryStore):
        failing_store = IntermittentlyFailingStore(failures_before_success=3)
        circuit_breaker = CircuitBreakerWrapper(
            key_value=failing_store,
            failure_threshold=3,
            recovery_timeout=0.1,
            success_threshold=2,
            error_types=(ConnectionError,),
        )

        # Store a value first (this will succeed after 3 failures)
        await failing_store.put(collection="test", key="test", value={"test": "value"})

        # Open the circuit with 3 failures
        for _ in range(3):
            with pytest.raises(ConnectionError):
                await circuit_breaker.get(collection="test", key="test")

        assert circuit_breaker._state == CircuitState.OPEN  # pyright: ignore[reportPrivateUsage]

        # Wait for recovery timeout
        import asyncio

        await asyncio.sleep(0.15)

        # Reset the failing store so next attempts succeed
        failing_store.failures_before_success = 0
        failing_store.reset_attempts()

        # First success in half-open
        result = await circuit_breaker.get(collection="test", key="test")
        assert result == {"test": "value"}
        assert circuit_breaker._state == CircuitState.HALF_OPEN  # pyright: ignore[reportPrivateUsage]
        assert circuit_breaker._success_count == 1  # pyright: ignore[reportPrivateUsage]

        # Second success should close the circuit
        result = await circuit_breaker.get(collection="test", key="test")
        assert result == {"test": "value"}
        assert circuit_breaker._state == CircuitState.CLOSED  # pyright: ignore[reportPrivateUsage]
        assert circuit_breaker._failure_count == 0  # pyright: ignore[reportPrivateUsage]
        assert circuit_breaker._success_count == 0  # pyright: ignore[reportPrivateUsage]

    async def test_circuit_resets_failure_count_on_success(self):
        failing_store = IntermittentlyFailingStore(failures_before_success=2)
        circuit_breaker = CircuitBreakerWrapper(
            key_value=failing_store, failure_threshold=5, recovery_timeout=1.0, error_types=(ConnectionError,)
        )

        await failing_store.put(collection="test", key="test", value={"test": "value"})

        # 2 failures
        for _ in range(2):
            failing_store.reset_attempts()
            with pytest.raises(ConnectionError):
                await circuit_breaker.get(collection="test", key="test")

        assert circuit_breaker._failure_count == 2  # pyright: ignore[reportPrivateUsage]

        # Success should reset failure count
        failing_store.failures_before_success = 0
        failing_store.reset_attempts()
        result = await circuit_breaker.get(collection="test", key="test")
        assert result == {"test": "value"}
        assert circuit_breaker._failure_count == 0  # pyright: ignore[reportPrivateUsage]

    async def test_circuit_only_counts_specified_error_types(self, memory_store: MemoryStore):
        class CustomError(Exception):
            pass

        class CustomFailingStore(MemoryStore):
            def __init__(self):
                super().__init__()
                self.call_count = 0

            async def get(self, key: str, *, collection: str | None = None):
                self.call_count += 1
                msg = "Custom error"
                raise CustomError(msg)

        failing_store = CustomFailingStore()
        circuit_breaker = CircuitBreakerWrapper(key_value=failing_store, failure_threshold=3, error_types=(ConnectionError, TimeoutError))

        # CustomError is not in error_types, so it should not count toward failures
        for _ in range(5):
            with pytest.raises(CustomError):
                await circuit_breaker.get(collection="test", key="test")

        # Circuit should still be closed
        assert circuit_breaker._state == CircuitState.CLOSED  # pyright: ignore[reportPrivateUsage]
        assert circuit_breaker._failure_count == 0  # pyright: ignore[reportPrivateUsage]

    async def test_circuit_all_operations_tracked(self, memory_store: MemoryStore):
        failing_store = IntermittentlyFailingStore(failures_before_success=10)
        circuit_breaker = CircuitBreakerWrapper(
            key_value=failing_store, failure_threshold=2, recovery_timeout=1.0, error_types=(ConnectionError,)
        )

        # Test that different operations all count toward circuit breaker state
        with pytest.raises(ConnectionError):
            await circuit_breaker.put(collection="test", key="test", value={"test": "value"})

        assert circuit_breaker._failure_count == 1  # pyright: ignore[reportPrivateUsage]

        with pytest.raises(ConnectionError):
            await circuit_breaker.delete(collection="test", key="test")

        assert circuit_breaker._failure_count == 2  # pyright: ignore[reportPrivateUsage]
        assert circuit_breaker._state == CircuitState.OPEN  # pyright: ignore[reportPrivateUsage]
