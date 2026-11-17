"""A custom store built only from public import paths (#335)."""

from typing_extensions import override

from key_value.aio._utils.managed_entry import ManagedEntry as InternalManagedEntry
from key_value.aio.stores.base import BaseStore, ManagedEntry


class DictStore(BaseStore):
    def __init__(self) -> None:
        self._entries: dict[tuple[str, str], ManagedEntry] = {}
        super().__init__(stable_api=True)

    @override
    async def _get_managed_entry(self, *, collection: str, key: str) -> ManagedEntry | None:
        return self._entries.get((collection, key))

    @override
    async def _put_managed_entry(self, *, key: str, collection: str, managed_entry: ManagedEntry) -> None:
        self._entries[(collection, key)] = managed_entry

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        return self._entries.pop((collection, key), None) is not None


def test_managed_entry_public_path_is_the_internal_class():
    assert ManagedEntry is InternalManagedEntry


async def test_custom_store_from_public_imports():
    store = DictStore()

    await store.put(collection="test", key="key", value={"v": 1}, ttl=100)

    assert await store.get(collection="test", key="key") == {"v": 1}
    _, ttl = await store.ttl(collection="test", key="key")
    assert ttl is not None
    assert await store.delete(collection="test", key="key") is True
    assert await store.get(collection="test", key="key") is None
