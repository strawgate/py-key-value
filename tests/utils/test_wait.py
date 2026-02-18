import asyncio

import pytest

from key_value.aio._utils.wait import async_wait_for_true


class SleepRecorder:
    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.calls.append(delay)


async def test_async_wait_for_true_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = SleepRecorder()
    monkeypatch.setattr(asyncio, "sleep", recorder)
    attempts = {"count": 0}

    async def bool_fn() -> bool:
        attempts["count"] += 1
        return attempts["count"] >= 2

    result = await async_wait_for_true(bool_fn=bool_fn, tries=3, wait_time=0.5)

    assert result is True
    assert recorder.calls == [0.5]


async def test_async_wait_for_true_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = SleepRecorder()
    monkeypatch.setattr(asyncio, "sleep", recorder)

    async def bool_fn() -> bool:
        return False

    result = await async_wait_for_true(bool_fn=bool_fn, tries=2, wait_time=0.25)

    assert result is False
    assert recorder.calls == [0.25]
