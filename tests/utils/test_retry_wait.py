import asyncio

import pytest

from key_value.aio._utils.retry import _calculate_delay, async_retry_operation
from key_value.aio._utils.wait import async_wait_for_true


class RetryableError(RuntimeError):
    """Custom retryable error for tests."""


async def _capture_sleep(delays: list[float], duration: float) -> None:
    delays.append(duration)


def test_calculate_delay_caps_at_max() -> None:
    assert _calculate_delay(initial_delay=0.5, max_delay=2.0, exponential_base=2.0, attempt=0) == 0.5
    assert _calculate_delay(initial_delay=0.5, max_delay=2.0, exponential_base=2.0, attempt=1) == 1.0
    assert _calculate_delay(initial_delay=0.5, max_delay=2.0, exponential_base=2.0, attempt=4) == 2.0


async def test_async_retry_operation_retries_and_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts: list[int] = []
    delays: list[float] = []

    async def operation() -> str:
        attempts.append(1)
        if len(attempts) < 3:
            raise RetryableError("retry")
        return "ok"

    monkeypatch.setattr(asyncio, "sleep", lambda duration: _capture_sleep(delays, duration))

    result = await async_retry_operation(
        max_retries=3,
        retry_on=(RetryableError,),
        initial_delay=0.5,
        max_delay=2.0,
        exponential_base=2.0,
        operation=operation,
    )

    assert result == "ok"
    assert len(attempts) == 3
    assert delays == [0.5, 1.0]


async def test_async_retry_operation_raises_after_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    delays: list[float] = []

    async def operation() -> str:
        raise RetryableError("fail")

    monkeypatch.setattr(asyncio, "sleep", lambda duration: _capture_sleep(delays, duration))

    with pytest.raises(RetryableError):
        await async_retry_operation(
            max_retries=1,
            retry_on=(RetryableError,),
            initial_delay=0.25,
            max_delay=1.0,
            exponential_base=2.0,
            operation=operation,
        )

    assert delays == [0.25]


async def test_async_retry_operation_skips_non_retryable_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    delays: list[float] = []

    async def operation() -> str:
        raise ValueError("no retry")

    monkeypatch.setattr(asyncio, "sleep", lambda duration: _capture_sleep(delays, duration))

    with pytest.raises(ValueError):
        await async_retry_operation(
            max_retries=2,
            retry_on=(RetryableError,),
            initial_delay=0.1,
            max_delay=1.0,
            exponential_base=2.0,
            operation=operation,
        )

    assert delays == []


async def test_async_wait_for_true_returns_true(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts: list[int] = []
    delays: list[float] = []

    async def bool_fn() -> bool:
        attempts.append(1)
        return len(attempts) >= 3

    monkeypatch.setattr(asyncio, "sleep", lambda duration: _capture_sleep(delays, duration))

    result = await async_wait_for_true(bool_fn=bool_fn, tries=5, wait_time=0.1)
    assert result is True
    assert len(attempts) == 3
    assert delays == [0.1, 0.1]


async def test_async_wait_for_true_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    delays: list[float] = []

    async def bool_fn() -> bool:
        return False

    monkeypatch.setattr(asyncio, "sleep", lambda duration: _capture_sleep(delays, duration))

    result = await async_wait_for_true(bool_fn=bool_fn, tries=3, wait_time=0.2)
    assert result is False
    assert delays == [0.2, 0.2, 0.2]
