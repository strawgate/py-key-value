from typing_extensions import override

from kv_store_adapter.stores.base import BaseStore
from kv_store_adapter.stores.utils.managed_entry import ManagedEntry


class NullStore(BaseStore):
    """Null object pattern store that accepts all operations but stores nothing."""

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        return None

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        pass

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        return False
