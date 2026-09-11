"""Tests for BaseWrapper's optional-capability forwarding."""

from pathlib import Path

import pytest

from key_value.aio.protocols import AsyncPutIfAbsentProtocol
from key_value.aio.stores.disk import DiskStore
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.logging import LoggingWrapper


class TestBaseWrapperPutIfAbsent:
    """BaseWrapper should forward put_if_absent when the wrapped store supports it."""

    async def test_wrapper_forwards_put_if_absent_when_supported(self):
        wrapped = LoggingWrapper(key_value=MemoryStore())

        assert isinstance(wrapped, AsyncPutIfAbsentProtocol)
        assert await wrapped.put_if_absent(collection="test", key="k", value={"data": "first"}) is True
        assert await wrapped.put_if_absent(collection="test", key="k", value={"data": "second"}) is False
        assert await wrapped.get(collection="test", key="k") == {"data": "first"}

    async def test_wrapper_raises_when_wrapped_store_does_not_support_it(self, tmp_path: Path):
        wrapped = LoggingWrapper(key_value=DiskStore(directory=tmp_path))

        # isinstance can't reflect the wrapped store's actual capability (Python's runtime-checkable
        # protocols only check the wrapper's own class), so the wrapper always satisfies the check --
        # the failure has to surface at call time instead.
        assert isinstance(wrapped, AsyncPutIfAbsentProtocol)

        with pytest.raises(NotImplementedError):
            await wrapped.put_if_absent(collection="test", key="k", value={"data": "value"})
