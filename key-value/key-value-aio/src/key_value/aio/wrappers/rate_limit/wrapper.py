import asyncio
import time
from collections import deque
from collections.abc import Mapping, Sequence
from typing import Any, Literal, SupportsFloat

from key_value.shared.errors.wrappers.rate_limit import RateLimitExceededError
from typing_extensions import override

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.wrappers.base import BaseWrapper


class RateLimitWrapper(BaseWrapper):
    """Wrapper that limits the rate of operations to protect backends from overload.

    This wrapper implements rate limiting using a sliding window algorithm to control
    the number of requests that can be made within a time window. This is essential for:
    - Protecting backends from being overwhelmed by too many requests
    - Complying with API rate limits of third-party services
    - Ensuring fair resource usage in multi-tenant environments
    - Preventing accidental DoS from application bugs

    The wrapper supports two strategies:
    - "sliding": More accurate, considers exact timestamps of recent requests
    - "fixed": Simpler, resets count at fixed intervals

    Example:
        rate_limiter = RateLimitWrapper(
            key_value=store,
            max_requests=100,       # Maximum 100 requests
            window_seconds=60.0,    # Per 60-second window
            strategy="sliding"      # Use sliding window
        )

        try:
            await rate_limiter.get(key="mykey")
        except RateLimitExceededError:
            # Too many requests, need to back off
            await asyncio.sleep(1)
    """

    def __init__(
        self,
        key_value: AsyncKeyValue,
        max_requests: int = 100,
        window_seconds: float = 60.0,
        strategy: Literal["sliding", "fixed"] = "sliding",
    ) -> None:
        """Initialize the rate limit wrapper.

        Args:
            key_value: The store to wrap.
            max_requests: Maximum number of requests allowed in the time window. Defaults to 100.
            window_seconds: Time window in seconds. Defaults to 60.0.
            strategy: Rate limiting strategy - "sliding" or "fixed". Defaults to "sliding".
        """
        self.key_value: AsyncKeyValue = key_value
        self.max_requests: int = max_requests
        self.window_seconds: float = window_seconds
        self.strategy: Literal["sliding", "fixed"] = strategy

        # For sliding window
        self._request_times: deque[float] = deque()
        self._lock: asyncio.Lock = asyncio.Lock()

        # For fixed window
        self._window_start: float | None = None
        self._request_count: int = 0

        super().__init__()

    async def _check_rate_limit_sliding(self) -> None:
        """Check rate limit using sliding window strategy."""
        async with self._lock:
            now = time.time()

            # Remove requests outside the current window
            while self._request_times and self._request_times[0] < now - self.window_seconds:
                self._request_times.popleft()

            # Check if we're at the limit
            if len(self._request_times) >= self.max_requests:
                raise RateLimitExceededError(
                    current_requests=len(self._request_times), max_requests=self.max_requests, window_seconds=self.window_seconds
                )

            # Record this request
            self._request_times.append(now)

    async def _check_rate_limit_fixed(self) -> None:
        """Check rate limit using fixed window strategy."""
        async with self._lock:
            now = time.time()

            # Check if we need to start a new window
            if self._window_start is None or now >= self._window_start + self.window_seconds:
                self._window_start = now
                self._request_count = 0

            # Check if we're at the limit
            if self._request_count >= self.max_requests:
                raise RateLimitExceededError(
                    current_requests=self._request_count, max_requests=self.max_requests, window_seconds=self.window_seconds
                )

            # Record this request
            self._request_count += 1

    async def _check_rate_limit(self) -> None:
        """Check rate limit based on configured strategy."""
        if self.strategy == "sliding":
            await self._check_rate_limit_sliding()
        else:
            await self._check_rate_limit_fixed()

    @override
    async def get(self, key: str, *, collection: str | None = None) -> dict[str, Any] | None:
        await self._check_rate_limit()
        return await self.key_value.get(key=key, collection=collection)

    @override
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[dict[str, Any] | None]:
        await self._check_rate_limit()
        return await self.key_value.get_many(keys=keys, collection=collection)

    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[dict[str, Any] | None, float | None]:
        await self._check_rate_limit()
        return await self.key_value.ttl(key=key, collection=collection)

    @override
    async def ttl_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[tuple[dict[str, Any] | None, float | None]]:
        await self._check_rate_limit()
        return await self.key_value.ttl_many(keys=keys, collection=collection)

    @override
    async def put(self, key: str, value: Mapping[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        await self._check_rate_limit()
        return await self.key_value.put(key=key, value=value, collection=collection, ttl=ttl)

    @override
    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[Mapping[str, Any]],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        await self._check_rate_limit()
        return await self.key_value.put_many(keys=keys, values=values, collection=collection, ttl=ttl)

    @override
    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        await self._check_rate_limit()
        return await self.key_value.delete(key=key, collection=collection)

    @override
    async def delete_many(self, keys: Sequence[str], *, collection: str | None = None) -> int:
        await self._check_rate_limit()
        return await self.key_value.delete_many(keys=keys, collection=collection)
