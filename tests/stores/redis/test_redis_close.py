from unittest.mock import AsyncMock

import pytest

from key_value.aio.stores.redis import RedisStore


@pytest.mark.parametrize("modern_client", [False, True], ids=["redis4-close", "redis5-aclose"])
async def test_owned_redis_client_cleanup(monkeypatch: pytest.MonkeyPatch, modern_client: bool) -> None:
    store = RedisStore(host="localhost")
    closer = AsyncMock()
    if modern_client:
        monkeypatch.setattr(store._client, "aclose", closer, raising=False)
    else:
        monkeypatch.setattr(store._client, "aclose", None, raising=False)
        monkeypatch.setattr(store._client, "close", closer)

    async with store:
        pass

    closer.assert_awaited_once_with()
