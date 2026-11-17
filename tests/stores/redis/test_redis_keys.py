from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, call

import pytest
from redis.asyncio import Redis

from key_value.aio.stores.redis import RedisStore
from key_value.aio.stores.redis.store import PAGE_LIMIT, _close_redis_client


@pytest.fixture
async def store() -> AsyncGenerator[RedisStore, None]:
    client = Redis(host="localhost", decode_responses=True)
    try:
        async with RedisStore(client=client) as store:
            yield store
    finally:
        await _close_redis_client(client)


async def test_keys_follows_cursor_through_empty_pages(monkeypatch: pytest.MonkeyPatch, store: RedisStore) -> None:
    scan = AsyncMock(side_effect=[(17, []), (9, ["small::first"]), (0, ["small::second"])])
    monkeypatch.setattr(store._client, "scan", scan)

    keys = await store.keys(collection="small")

    assert keys == ["first", "second"]
    assert scan.await_args_list == [
        call(cursor=0, match="small::*", count=PAGE_LIMIT),
        call(cursor=17, match="small::*", count=PAGE_LIMIT),
        call(cursor=9, match="small::*", count=PAGE_LIMIT),
    ]


async def test_keys_does_not_count_duplicates_toward_limit(monkeypatch: pytest.MonkeyPatch, store: RedisStore) -> None:
    scan = AsyncMock(side_effect=[(7, ["small::first"]), (3, ["small::first"]), (0, ["small::second"])])
    monkeypatch.setattr(store._client, "scan", scan)

    keys = await store.keys(collection="small", limit=2)

    assert keys == ["first", "second"]
    assert scan.await_count == 3


async def test_keys_stops_at_limit_before_cursor_finishes(monkeypatch: pytest.MonkeyPatch, store: RedisStore) -> None:
    scan = AsyncMock(side_effect=[(7, ["small::first"]), (3, ["small::second"]), (0, ["small::third"])])
    monkeypatch.setattr(store._client, "scan", scan)

    keys = await store.keys(collection="small", limit=2)

    assert keys == ["first", "second"]
    assert scan.await_count == 2


async def test_keys_enforces_limit_when_scan_returns_more_than_count(monkeypatch: pytest.MonkeyPatch, store: RedisStore) -> None:
    scan = AsyncMock(return_value=(0, ["small::first", "small::second", "small::third"]))
    monkeypatch.setattr(store._client, "scan", scan)

    keys = await store.keys(collection="small", limit=2)

    assert keys == ["first", "second"]
    scan.assert_awaited_once_with(cursor=0, match="small::*", count=2)


@pytest.mark.parametrize("limit", [None, 0, PAGE_LIMIT + 1])
async def test_keys_preserves_default_and_maximum_limit(monkeypatch: pytest.MonkeyPatch, store: RedisStore, limit: int | None) -> None:
    scan = AsyncMock(return_value=(0, []))
    monkeypatch.setattr(store._client, "scan", scan)

    assert await store.keys(collection="small", limit=limit) == []

    scan.assert_awaited_once_with(cursor=0, match="small::*", count=PAGE_LIMIT)
