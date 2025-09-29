from collections.abc import Awaitable, Callable

from key_value.shared.code_gen.sleep import asleep, sleep


async def async_wait_for_true(bool_fn: Callable[[], Awaitable[bool]], tries: int = 10, wait_time: float = 1) -> bool:
    """
    Wait for a store to be ready.
    """
    for _ in range(tries):
        if await bool_fn():
            return True
        await asleep(seconds=wait_time)
    return False


def wait_for_true(bool_fn: Callable[[], bool], tries: int = 10, wait_time: float = 1) -> bool:
    """
    Wait for a store to be ready.
    """
    for _ in range(tries):
        if bool_fn():
            return True
        sleep(seconds=wait_time)
    return False
