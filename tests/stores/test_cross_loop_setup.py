import asyncio
import time
from collections.abc import Coroutine
from queue import Queue
from threading import Event, Thread
from typing import Any

import pytest

from key_value.aio._utils.managed_entry import ManagedEntry
from key_value.aio.errors import StoreSetupError
from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.memory import MemoryStore


class BlockingSetupStore(BaseStore):
    def __init__(self) -> None:
        self.setup_calls = 0
        self.setup_started = Event()
        self.release_setup = Event()
        super().__init__(stable_api=True)

    async def _setup(self) -> None:
        self.setup_calls += 1
        self.setup_started.set()
        await asyncio.to_thread(self.release_setup.wait)

    async def _get_managed_entry(self, *, collection: str, key: str) -> ManagedEntry | None:
        return None

    async def _put_managed_entry(self, *, collection: str, key: str, managed_entry: ManagedEntry) -> None:
        return None

    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        return False


class BlockingCollectionStore(MemoryStore):
    def __init__(self) -> None:
        self.collection_setup_calls = 0
        self.collection_setup_started = Event()
        self.release_collection_setup = Event()
        super().__init__()

    async def _setup_collection(self, *, collection: str) -> None:
        self.collection_setup_calls += 1
        self.collection_setup_started.set()
        await asyncio.to_thread(self.release_collection_setup.wait)
        await super()._setup_collection(collection=collection)


class BlockingSeedStore(BaseStore):
    def __init__(self) -> None:
        self.entries: dict[tuple[str, str], ManagedEntry] = {}
        self.seed_started = Event()
        self.release_seed = Event()
        super().__init__(seed={"shared": {"seeded": {"value": 1}}}, stable_api=True)

    async def _get_managed_entry(self, *, collection: str, key: str) -> ManagedEntry | None:
        return self.entries.get((collection, key))

    async def _put_managed_entry(self, *, collection: str, key: str, managed_entry: ManagedEntry) -> None:
        self.seed_started.set()
        await asyncio.to_thread(self.release_seed.wait)
        self.entries[(collection, key)] = managed_entry

    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        return self.entries.pop((collection, key), None) is not None


class FailingSeedStore(BlockingSeedStore):
    def __init__(self) -> None:
        self.setup_calls = 0
        super().__init__()
        self.release_seed.set()

    async def _setup(self) -> None:
        self.setup_calls += 1

    async def _put_managed_entry(self, *, collection: str, key: str, managed_entry: ManagedEntry) -> None:
        msg = "seed failed"
        raise ValueError(msg)


def _run_concurrently(
    operation: Coroutine[Any, Any, None],
    second_operation: Coroutine[Any, Any, None],
    *,
    started: Event,
    release: Event,
) -> list[BaseException]:
    errors: Queue[BaseException] = Queue()

    def run(coroutine: Coroutine[Any, Any, None]) -> None:
        try:
            asyncio.run(coroutine)
        except BaseException as error:
            errors.put(error)

    threads = [Thread(target=run, args=(operation,), daemon=True), Thread(target=run, args=(second_operation,), daemon=True)]
    threads[0].start()
    assert started.wait(timeout=5)
    threads[1].start()
    time.sleep(0.05)
    release.set()

    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()

    collected_errors: list[BaseException] = []
    while not errors.empty():
        collected_errors.append(errors.get_nowait())
    return collected_errors


def test_store_setup_is_coordinated_across_event_loops() -> None:
    store = BlockingSetupStore()

    errors = _run_concurrently(
        store.setup(),
        store.setup(),
        started=store.setup_started,
        release=store.release_setup,
    )

    assert errors == []
    assert store.setup_calls == 1


def test_collection_setup_is_coordinated_across_event_loops() -> None:
    store = BlockingCollectionStore()

    errors = _run_concurrently(
        store.setup_collection(collection="shared"),
        store.setup_collection(collection="shared"),
        started=store.collection_setup_started,
        release=store.release_collection_setup,
    )

    assert errors == []
    assert store.collection_setup_calls == 1


async def test_cancelled_setup_can_be_retried() -> None:
    store = BlockingSetupStore()
    setup_task = asyncio.create_task(store.setup())
    assert await asyncio.to_thread(store.setup_started.wait, 5)

    setup_task.cancel()
    store.release_setup.set()
    with pytest.raises(asyncio.CancelledError):
        await setup_task

    await store.setup()
    assert store.setup_calls == 2


async def test_cancelled_waiter_does_not_cancel_setup() -> None:
    store = BlockingSetupStore()
    setup_task = asyncio.create_task(store.setup())
    assert await asyncio.to_thread(store.setup_started.wait, 5)

    waiting_task = asyncio.create_task(store.setup())
    await asyncio.sleep(0)
    waiting_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting_task

    store.release_setup.set()
    await setup_task
    assert store.setup_calls == 1


async def test_setup_waits_until_seed_data_is_ready() -> None:
    store = BlockingSeedStore()
    setup_task = asyncio.create_task(store.setup())
    assert await asyncio.to_thread(store.seed_started.wait, 5)

    get_task = asyncio.create_task(store.get(collection="shared", key="seeded"))
    await asyncio.sleep(0)
    assert not get_task.done()

    store.release_seed.set()
    await setup_task
    assert await get_task == {"value": 1}


async def test_seed_failure_is_terminal_for_the_store_instance() -> None:
    store = FailingSeedStore()

    with pytest.raises(StoreSetupError, match="seed failed"):
        await store.setup()
    with pytest.raises(StoreSetupError, match="seed failed"):
        await store.setup()

    assert store.setup_calls == 1
