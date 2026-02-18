import asyncio

import pytest

from key_value.aio._utils.retry import _calculate_delay, async_retry_operation


class SleepRecorder:
    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.calls.append(delay)


@pytest.mark.parametrize(
    ("initial_delay", "max_delay", "exponential_base", "attempt", "expected"),
    [
        (1.0, 10.0, 2.0, 0, 1.0),
        (1.0, 10.0, 2.0, 3, 8.0),
        (1.0, 10.0, 2.0, 4, 10.0),
    ],
    ids=["initial", "exponential", "capped"],
)
def test_calculate_delay(initial_delay: float, max_delay: float, exponential_base: float, attempt: int, expected: float) -> None:
    assert _calculate_delay(initial_delay, max_delay, exponential_base, attempt) == expected


async def test_async_retry_operation_success_no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = SleepRecorder()
    monkeypatch.setattr(asyncio, "sleep", recorder)

    async def operation() -> str:
        return "ok"

    result = await async_retry_operation(
        max_retries=2,
        retry_on=(ValueError,),
        initial_delay=1.0,
        max_delay=10.0,
        exponential_base=2.0,
        operation=operation,
    )

    assert result == "ok"
    assert recorder.calls == []


async def test_async_retry_operation_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = SleepRecorder()
    monkeypatch.setattr(asyncio, "sleep", recorder)
    attempts = {"count": 0}

    async def operation() -> str:
        if attempts["count"] < 2:
            attempts["count"] += 1
            raise ValueError("fail")
        return "ok"

    result = await async_retry_operation(
        max_retries=3,
        retry_on=(ValueError,),
        initial_delay=1.0,
        max_delay=10.0,
        exponential_base=2.0,
        operation=operation,
    )

    assert result == "ok"
    assert recorder.calls == [1.0, 2.0]


async def test_async_retry_operation_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = SleepRecorder()
    monkeypatch.setattr(asyncio, "sleep", recorder)

    async def operation() -> str:
        raise ValueError("fail")

    with pytest.raises(ValueError, match="fail"):
        await async_retry_operation(
            max_retries=1,
            retry_on=(ValueError,),
            initial_delay=1.0,
            max_delay=10.0,
            exponential_base=2.0,
            operation=operation,
        )

    assert recorder.calls == [1.0]


async def test_async_retry_operation_no_retry_on_other_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = SleepRecorder()
    monkeypatch.setattr(asyncio, "sleep", recorder)

    async def operation() -> str:
        raise TypeError("bad")

    with pytest.raises(TypeError, match="bad"):
        await async_retry_operation(
            max_retries=2,
            retry_on=(ValueError,),
            initial_delay=1.0,
            max_delay=10.0,
            exponential_base=2.0,
            operation=operation,
        )

    assert recorder.calls == []
