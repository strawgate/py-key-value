import pytest

from key_value.aio._utils.managed_entry import ManagedEntry
from key_value.aio.errors import StoreSetupError
from key_value.aio.stores.base import BaseContextManagerStore


# SetupFailsStore intentionally does not use ContextManagerStoreTestMixin:
# test_setup_failure_closes_registered_exit_stack_callbacks covers __aenter__ failure cleanup,
# while the mixin assumes the fixture can be entered successfully.
class SetupFailsStore(BaseContextManagerStore):
    cleanup_called: bool

    def __init__(self) -> None:
        self.cleanup_called = False
        super().__init__(stable_api=True)

    async def _cleanup(self) -> None:
        self.cleanup_called = True

    async def _setup(self) -> None:
        self._exit_stack.push_async_callback(self._cleanup)
        msg = "setup failed"
        raise ValueError(msg)

    async def _get_managed_entry(self, *, collection: str, key: str) -> ManagedEntry | None:
        return None

    async def _put_managed_entry(self, *, collection: str, key: str, managed_entry: ManagedEntry) -> None:
        return None

    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        return False


async def test_setup_failure_closes_registered_exit_stack_callbacks() -> None:
    store = SetupFailsStore()

    with pytest.raises(StoreSetupError, match="setup failed"):
        async with store:
            pass

    assert store.cleanup_called is True
    assert store._exit_stack_entered is False
