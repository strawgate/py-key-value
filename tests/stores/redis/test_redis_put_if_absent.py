from typing import cast
from unittest.mock import AsyncMock

from redis.asyncio import Redis

from key_value.aio.stores.redis.store import _redis_set_if_absent


async def test_redis_set_if_absent_uses_atomic_set() -> None:
    client = AsyncMock(spec=Redis)
    set_mock = AsyncMock(return_value=True)
    client.configure_mock(set=set_mock)

    stored = await _redis_set_if_absent(
        cast("Redis", client),
        "collection::key",
        '{"value": 1}',
        0.5,
    )

    assert stored is True
    set_mock.assert_awaited_once_with(
        name="collection::key",
        value='{"value": 1}',
        nx=True,
        px=500,
    )
