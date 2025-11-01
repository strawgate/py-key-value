import asyncio
from collections.abc import Callable, Coroutine, Mapping, Sequence
from typing import Any, SupportsFloat, TypeVar

from key_value.shared.errors.wrappers.bulkhead import BulkheadFullError
from typing_extensions import override

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.wrappers.base import BaseWrapper

T = TypeVar("T")


class BulkheadWrapper(BaseWrapper):
    """Wrapper that implements the bulkhead pattern to isolate operations with resource pools.

    This wrapper limits the number of concurrent operations and queued operations to prevent
    resource exhaustion and isolate failures. The bulkhead pattern is inspired by ship bulkheads
    that prevent a single hull breach from sinking the entire ship.

    Benefits:
    - Prevents a single slow or failing backend from consuming all resources
    - Limits concurrent requests to protect backend from overload
    - Provides bounded queue to prevent unbounded memory growth
    - Enables graceful degradation under high load

    Example:
        bulkhead = BulkheadWrapper(
            key_value=store,
            max_concurrent=10,    # Max 10 concurrent operations
            max_waiting=20,       # Max 20 operations can wait in queue
        )

        try:
            await bulkhead.get(key="mykey")
        except BulkheadFullError:
            # Too many concurrent operations, system is overloaded
            # Handle gracefully (return cached value, error response, etc.)
            pass
    """

    def __init__(
        self,
        key_value: AsyncKeyValue,
        max_concurrent: int = 10,
        max_waiting: int = 20,
    ) -> None:
        """Initialize the bulkhead wrapper.

        Args:
            key_value: The store to wrap.
            max_concurrent: Maximum number of concurrent operations. Defaults to 10.
            max_waiting: Maximum number of operations that can wait in queue. Defaults to 20.
        """
        self.key_value: AsyncKeyValue = key_value
        self.max_concurrent: int = max_concurrent
        self.max_waiting: int = max_waiting

        # Use semaphore to limit concurrent operations
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(max_concurrent)
        self._waiting_count: int = 0
        self._waiting_lock: asyncio.Lock = asyncio.Lock()

        super().__init__()

    async def _execute_with_bulkhead(self, operation: Callable[..., Coroutine[Any, Any, T]], *args: Any, **kwargs: Any) -> T:
        """Execute an operation with bulkhead resource limiting."""
        # Check if we can accept this operation
        async with self._waiting_lock:
            if self._waiting_count >= self.max_waiting:
                raise BulkheadFullError(max_concurrent=self.max_concurrent, max_waiting=self.max_waiting)
            self._waiting_count += 1

        try:
            # Acquire semaphore to limit concurrency
            async with self._semaphore:
                # Once we have the semaphore, we're no longer waiting
                async with self._waiting_lock:
                    self._waiting_count -= 1

                # Execute the operation
                return await operation(*args, **kwargs)
        except Exception:
            # Make sure to decrement waiting count if we error before acquiring semaphore
            async with self._waiting_lock:
                # Only decrement if we're still counted as waiting
                # (might have already decremented if we got the semaphore)
                if self._waiting_count > 0 and self._semaphore.locked():
                    self._waiting_count -= 1
            raise

    @override
    async def get(self, key: str, *, collection: str | None = None) -> dict[str, Any] | None:
        return await self._execute_with_bulkhead(self.key_value.get, key=key, collection=collection)

    @override
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[dict[str, Any] | None]:
        return await self._execute_with_bulkhead(self.key_value.get_many, keys=keys, collection=collection)

    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[dict[str, Any] | None, float | None]:
        return await self._execute_with_bulkhead(self.key_value.ttl, key=key, collection=collection)

    @override
    async def ttl_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[tuple[dict[str, Any] | None, float | None]]:
        return await self._execute_with_bulkhead(self.key_value.ttl_many, keys=keys, collection=collection)

    @override
    async def put(self, key: str, value: Mapping[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        return await self._execute_with_bulkhead(self.key_value.put, key=key, value=value, collection=collection, ttl=ttl)

    @override
    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[Mapping[str, Any]],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        return await self._execute_with_bulkhead(self.key_value.put_many, keys=keys, values=values, collection=collection, ttl=ttl)

    @override
    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        return await self._execute_with_bulkhead(self.key_value.delete, key=key, collection=collection)

    @override
    async def delete_many(self, keys: Sequence[str], *, collection: str | None = None) -> int:
        return await self._execute_with_bulkhead(self.key_value.delete_many, keys=keys, collection=collection)
