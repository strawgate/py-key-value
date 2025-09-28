import asyncio
import time


async def asleep(seconds: float) -> None:
    """
    Equivalent to asyncio.sleep(), converted to time.sleep() by async_to_sync.
    """
    await asyncio.sleep(seconds)


def sleep(seconds: float) -> None:
    """
    Equivalent to time.sleep(), converted to asyncio.sleep() by async_to_sync.
    """
    time.sleep(seconds)
