import asyncio
from collections.abc import Awaitable, Callable
from typing import SupportsFloat


async def async_wait_for_true(bool_fn: Callable[[], Awaitable[bool]], tries: int = 10, wait_time: SupportsFloat = 1) -> bool:
    """
    Wait for a store to be ready.
    """
    for _ in range(tries):
        if await bool_fn():
            return True
        await asyncio.sleep(float(wait_time))
    return False
