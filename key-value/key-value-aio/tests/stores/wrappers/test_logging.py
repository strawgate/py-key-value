import logging
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from _pytest.logging import LogCaptureFixture
from inline_snapshot import snapshot
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.logging import LoggingWrapper
from tests.stores.base import BaseStoreTests


def get_messages_from_caplog(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [record.message for record in caplog.records]


class TestLoggingWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> LoggingWrapper:
        return LoggingWrapper(key_value=MemoryStore(max_entries_per_collection=500), log_level=logging.INFO)

    @override
    @pytest.fixture
    async def structured_logs_store(self) -> LoggingWrapper:
        return LoggingWrapper(key_value=MemoryStore(max_entries_per_collection=500), log_level=logging.INFO, structured_logs=True)

    @pytest.fixture
    async def capture_logs(self, caplog: pytest.LogCaptureFixture) -> AsyncGenerator[LogCaptureFixture, Any]:
        with caplog.at_level(logging.INFO):
            yield caplog

    async def test_logging_get_operations(
        self, store: LoggingWrapper, structured_logs_store: LoggingWrapper, capture_logs: LogCaptureFixture
    ):
        await store.get(collection="test", key="test")
        assert get_messages_from_caplog(capture_logs) == snapshot(
            [
                "Start GET collection='test' keys='test'",
                "Finish GET collection='test' keys='test' ({'hit': False})",
            ]
        )

        capture_logs.clear()

        await structured_logs_store.get(collection="test", key="test")
        assert get_messages_from_caplog(capture_logs) == snapshot(
            [
                '{"status": "start", "action": "GET", "collection": "test", "keys": "test"}',
                '{"status": "finish", "action": "GET", "collection": "test", "keys": "test", "extra": {"hit": false}}',
            ]
        )

        capture_logs.clear()

        await store.get_many(collection="test", keys=["test", "test_2"])
        assert get_messages_from_caplog(capture_logs) == snapshot(
            [
                "Start GET_MANY collection='test' keys='['test', 'test_2']' ({'keys': ['test', 'test_2']})",
                "Finish GET_MANY collection='test' keys='['test', 'test_2']' ({'hits': 0, 'misses': 2})",
            ]
        )

        capture_logs.clear()

        await structured_logs_store.get_many(collection="test", keys=["test", "test_2"])
        assert get_messages_from_caplog(capture_logs) == snapshot(
            [
                '{"status": "start", "action": "GET_MANY", "collection": "test", "keys": ["test", "test_2"], "extra": {"keys": ["test", "test_2"]}}',
                '{"status": "finish", "action": "GET_MANY", "collection": "test", "keys": ["test", "test_2"], "extra": {"hits": 0, "misses": 2}}',
            ]
        )

    async def test_logging_put_operations(
        self, store: LoggingWrapper, structured_logs_store: LoggingWrapper, capture_logs: LogCaptureFixture
    ):
        logging_store = LoggingWrapper(key_value=store, log_level=logging.INFO)

        await logging_store.put(collection="test", key="test", value={"test": "value"})
        assert get_messages_from_caplog(capture_logs) == snapshot(
            [
                "Start PUT collection='test' keys='test' value={'test': 'value'} ({'ttl': None})",
                "Start PUT collection='test' keys='test' value={'test': 'value'} ({'ttl': None})",
                "Finish PUT collection='test' keys='test' value={'test': 'value'} ({'ttl': None})",
                "Finish PUT collection='test' keys='test' value={'test': 'value'} ({'ttl': None})",
            ]
        )

        capture_logs.clear()

        await structured_logs_store.put(collection="test", key="test", value={"test": "value"})
        assert get_messages_from_caplog(capture_logs) == snapshot(
            [
                '{"status": "start", "action": "PUT", "collection": "test", "keys": "test", "value": {"test": "value"}, "extra": {"ttl": null}}',
                '{"status": "finish", "action": "PUT", "collection": "test", "keys": "test", "value": {"test": "value"}, "extra": {"ttl": null}}',
            ]
        )

        capture_logs.clear()

        await logging_store.put_many(collection="test", keys=["test", "test_2"], values=[{"test": "value"}, {"test": "value_2"}])
        assert get_messages_from_caplog(capture_logs) == snapshot(
            [
                "Start PUT_MANY collection='test' keys='['test', 'test_2']' value=[{'test': 'value'}, {'test': 'value_2'}] ({'ttl': None})",
                "Start PUT_MANY collection='test' keys='['test', 'test_2']' value=[{'test': 'value'}, {'test': 'value_2'}] ({'ttl': None})",
                "Finish PUT_MANY collection='test' keys='['test', 'test_2']' value=[{'test': 'value'}, {'test': 'value_2'}] ({'ttl': None})",
                "Finish PUT_MANY collection='test' keys='['test', 'test_2']' value=[{'test': 'value'}, {'test': 'value_2'}] ({'ttl': None})",
            ]
        )

        capture_logs.clear()

        await structured_logs_store.put_many(collection="test", keys=["test", "test_2"], values=[{"test": "value"}, {"test": "value_2"}])
        assert get_messages_from_caplog(capture_logs) == snapshot(
            [
                '{"status": "start", "action": "PUT_MANY", "collection": "test", "keys": ["test", "test_2"], "value": [{"test": "value"}, {"test": "value_2"}], "extra": {"ttl": null}}',
                '{"status": "finish", "action": "PUT_MANY", "collection": "test", "keys": ["test", "test_2"], "value": [{"test": "value"}, {"test": "value_2"}], "extra": {"ttl": null}}',
            ]
        )

    async def test_logging_delete_operations(
        self, store: LoggingWrapper, structured_logs_store: LoggingWrapper, capture_logs: LogCaptureFixture
    ):
        logging_store = LoggingWrapper(key_value=store, log_level=logging.INFO)

        await logging_store.delete(collection="test", key="test")
        assert get_messages_from_caplog(capture_logs) == snapshot(
            [
                "Start DELETE collection='test' keys='test'",
                "Start DELETE collection='test' keys='test'",
                "Finish DELETE collection='test' keys='test' ({'deleted': False})",
                "Finish DELETE collection='test' keys='test' ({'deleted': False})",
            ]
        )

        capture_logs.clear()

        await structured_logs_store.delete(collection="test", key="test")
        assert get_messages_from_caplog(capture_logs) == snapshot(
            [
                '{"status": "start", "action": "DELETE", "collection": "test", "keys": "test"}',
                '{"status": "finish", "action": "DELETE", "collection": "test", "keys": "test", "extra": {"deleted": false}}',
            ]
        )

        capture_logs.clear()

        await logging_store.delete_many(collection="test", keys=["test", "test_2"])
        assert get_messages_from_caplog(capture_logs) == snapshot(
            [
                "Start DELETE_MANY collection='test' keys='['test', 'test_2']' ({'keys': ['test', 'test_2']})",
                "Start DELETE_MANY collection='test' keys='['test', 'test_2']' ({'keys': ['test', 'test_2']})",
                "Finish DELETE_MANY collection='test' keys='['test', 'test_2']' ({'deleted': 0})",
                "Finish DELETE_MANY collection='test' keys='['test', 'test_2']' ({'deleted': 0})",
            ]
        )

        capture_logs.clear()

        await structured_logs_store.delete_many(collection="test", keys=["test", "test_2"])
        assert get_messages_from_caplog(capture_logs) == snapshot(
            [
                '{"status": "start", "action": "DELETE_MANY", "collection": "test", "keys": ["test", "test_2"], "extra": {"keys": ["test", "test_2"]}}',
                '{"status": "finish", "action": "DELETE_MANY", "collection": "test", "keys": ["test", "test_2"], "extra": {"deleted": 0}}',
            ]
        )

    async def test_put_get_delete_get_logging(
        self, store: LoggingWrapper, structured_logs_store: LoggingWrapper, capture_logs: LogCaptureFixture
    ):
        await store.put(collection="test", key="test", value={"test": "value"})
        assert await store.get(collection="test", key="test") == {"test": "value"}
        assert await store.delete(collection="test", key="test")
        assert await store.get(collection="test", key="test") is None

        assert get_messages_from_caplog(capture_logs) == snapshot(
            [
                "Start PUT collection='test' keys='test' value={'test': 'value'} ({'ttl': None})",
                "Finish PUT collection='test' keys='test' value={'test': 'value'} ({'ttl': None})",
                "Start GET collection='test' keys='test'",
                "Finish GET collection='test' keys='test' value={'test': 'value'} ({'hit': True})",
                "Start DELETE collection='test' keys='test'",
                "Finish DELETE collection='test' keys='test' ({'deleted': True})",
                "Start GET collection='test' keys='test'",
                "Finish GET collection='test' keys='test' ({'hit': False})",
            ]
        )

        capture_logs.clear()

        await structured_logs_store.put(collection="test", key="test", value={"test": "value"})
        assert await structured_logs_store.get(collection="test", key="test") == {"test": "value"}
        assert await structured_logs_store.delete(collection="test", key="test")
        assert await structured_logs_store.get(collection="test", key="test") is None

        assert get_messages_from_caplog(capture_logs) == snapshot(
            [
                '{"status": "start", "action": "PUT", "collection": "test", "keys": "test", "value": {"test": "value"}, "extra": {"ttl": null}}',
                '{"status": "finish", "action": "PUT", "collection": "test", "keys": "test", "value": {"test": "value"}, "extra": {"ttl": null}}',
                '{"status": "start", "action": "GET", "collection": "test", "keys": "test"}',
                '{"status": "finish", "action": "GET", "collection": "test", "keys": "test", "value": {"test": "value"}, "extra": {"hit": true}}',
                '{"status": "start", "action": "DELETE", "collection": "test", "keys": "test"}',
                '{"status": "finish", "action": "DELETE", "collection": "test", "keys": "test", "extra": {"deleted": true}}',
                '{"status": "start", "action": "GET", "collection": "test", "keys": "test"}',
                '{"status": "finish", "action": "GET", "collection": "test", "keys": "test", "extra": {"hit": false}}',
            ]
        )
