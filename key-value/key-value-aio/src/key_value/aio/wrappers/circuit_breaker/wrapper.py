import time
from collections.abc import Callable, Coroutine, Mapping, Sequence
from enum import Enum
from typing import Any, SupportsFloat, TypeVar

from key_value.shared.errors.wrappers.circuit_breaker import CircuitOpenError
from typing_extensions import override

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.wrappers.base import BaseWrapper

T = TypeVar("T")


class CircuitState(Enum):
    """States for the circuit breaker."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, blocking requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreakerWrapper(BaseWrapper):
    """Wrapper that implements the circuit breaker pattern to prevent cascading failures.

    This wrapper tracks operation failures and opens the circuit after a threshold of consecutive
    failures. When the circuit is open, requests are blocked immediately without attempting the
    operation. After a recovery timeout, the circuit moves to half-open state to test if the
    backend has recovered.

    The circuit breaker pattern is essential for production resilience as it:
    - Prevents cascading failures when a backend becomes unhealthy
    - Reduces load on failing backends, giving them time to recover
    - Provides fast failure responses instead of waiting for timeouts
    - Automatically attempts recovery after a configured timeout

    Example:
        circuit_breaker = CircuitBreakerWrapper(
            key_value=store,
            failure_threshold=5,        # Open after 5 consecutive failures
            recovery_timeout=30.0,      # Try recovery after 30 seconds
            success_threshold=2,        # Close after 2 successes in half-open
        )

        try:
            value = await circuit_breaker.get(key="mykey")
        except CircuitOpenError:
            # Circuit is open, backend is considered unhealthy
            # Handle gracefully (use cache, return default, etc.)
            pass
    """

    def __init__(
        self,
        key_value: AsyncKeyValue,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        success_threshold: int = 2,
        error_types: tuple[type[Exception], ...] = (Exception,),
    ) -> None:
        """Initialize the circuit breaker wrapper.

        Args:
            key_value: The store to wrap.
            failure_threshold: Number of consecutive failures before opening the circuit. Defaults to 5.
            recovery_timeout: Seconds to wait before attempting recovery (moving to half-open). Defaults to 30.0.
            success_threshold: Number of consecutive successes in half-open state before closing the circuit. Defaults to 2.
            error_types: Tuple of exception types that count as failures. Defaults to (Exception,).
        """
        self.key_value: AsyncKeyValue = key_value
        self.failure_threshold: int = failure_threshold
        self.recovery_timeout: float = recovery_timeout
        self.success_threshold: int = success_threshold
        self.error_types: tuple[type[Exception], ...] = error_types

        # Circuit state
        self._state: CircuitState = CircuitState.CLOSED
        self._failure_count: int = 0
        self._success_count: int = 0
        self._last_failure_time: float | None = None

        super().__init__()

    def _check_circuit(self) -> None:
        """Check the circuit state and potentially transition states."""
        if self._state == CircuitState.OPEN:
            # Check if we should move to half-open
            if self._last_failure_time is not None and time.time() - self._last_failure_time >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
            else:
                # Circuit is still open, raise error
                raise CircuitOpenError(failure_count=self._failure_count, last_failure_time=self._last_failure_time)

    def _on_success(self) -> None:
        """Handle successful operation."""
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.success_threshold:
                # Close the circuit
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._success_count = 0
        elif self._state == CircuitState.CLOSED:
            # Reset failure count on success
            self._failure_count = 0

    def _on_failure(self) -> None:
        """Handle failed operation."""
        self._last_failure_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            # Failed in half-open, go back to open
            self._state = CircuitState.OPEN
            self._success_count = 0
        elif self._state == CircuitState.CLOSED:
            self._failure_count += 1
            if self._failure_count >= self.failure_threshold:
                # Open the circuit
                self._state = CircuitState.OPEN

    async def _execute_with_circuit_breaker(self, operation: Callable[..., Coroutine[Any, Any, T]], *args: Any, **kwargs: Any) -> T:
        """Execute an operation with circuit breaker logic."""
        self._check_circuit()

        try:
            result = await operation(*args, **kwargs)
        except self.error_types:
            self._on_failure()
            raise
        else:
            self._on_success()
            return result

    @override
    async def get(self, key: str, *, collection: str | None = None) -> dict[str, Any] | None:
        return await self._execute_with_circuit_breaker(self.key_value.get, key=key, collection=collection)

    @override
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[dict[str, Any] | None]:
        return await self._execute_with_circuit_breaker(self.key_value.get_many, keys=keys, collection=collection)

    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[dict[str, Any] | None, float | None]:
        return await self._execute_with_circuit_breaker(self.key_value.ttl, key=key, collection=collection)

    @override
    async def ttl_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[tuple[dict[str, Any] | None, float | None]]:
        return await self._execute_with_circuit_breaker(self.key_value.ttl_many, keys=keys, collection=collection)

    @override
    async def put(self, key: str, value: Mapping[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        return await self._execute_with_circuit_breaker(self.key_value.put, key=key, value=value, collection=collection, ttl=ttl)

    @override
    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[Mapping[str, Any]],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        return await self._execute_with_circuit_breaker(self.key_value.put_many, keys=keys, values=values, collection=collection, ttl=ttl)

    @override
    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        return await self._execute_with_circuit_breaker(self.key_value.delete, key=key, collection=collection)

    @override
    async def delete_many(self, keys: Sequence[str], *, collection: str | None = None) -> int:
        return await self._execute_with_circuit_breaker(self.key_value.delete_many, keys=keys, collection=collection)
