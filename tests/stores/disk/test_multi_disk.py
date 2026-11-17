import json
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from dirty_equals import IsDatetime
from diskcache import Cache
from inline_snapshot import snapshot
from typing_extensions import override

from key_value.aio.errors import StoreSetupError
from key_value.aio.stores.disk.multi_store import MultiDiskStore
from key_value.aio.stores.disk.store import _disk_cache_clear
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

TEST_SIZE_LIMIT = 100 * 1024  # 100KB


class TestMultiDiskStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, per_test_temp_dir: Path) -> AsyncGenerator[MultiDiskStore, None]:
        store = MultiDiskStore(base_directory=per_test_temp_dir, max_size=TEST_SIZE_LIMIT)

        yield store

        # Wipe the store after returning it
        for collection in store._cache:
            _disk_cache_clear(cache=store._cache[collection])

    async def test_value_stored(self, store: MultiDiskStore):
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})
        disk_cache: Cache = store._cache["test"]

        value = disk_cache.get(key="test_key")
        value_as_dict = json.loads(value)
        assert value_as_dict == snapshot(
            {
                "collection": "test",
                "value": {"name": "Alice", "age": 30},
                "key": "test_key",
                "created_at": IsDatetime(iso_string=True),
                "version": 1,
            }
        )

        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30}, ttl=10)

        value = disk_cache.get(key="test_key")
        value_as_dict = json.loads(value)
        assert value_as_dict == snapshot(
            {
                "collection": "test",
                "created_at": IsDatetime(iso_string=True),
                "value": {"age": 30, "name": "Alice"},
                "key": "test_key",
                "expires_at": IsDatetime(iso_string=True),
                "version": 1,
            }
        )

    @pytest.mark.parametrize("collection", [".", "..", "/"])
    @pytest.mark.parametrize("auto_create", [True, False])
    async def test_reject_collection_directory_outside_or_equal_to_base(self, per_test_temp_dir: Path, collection: str, auto_create: bool):
        base_directory = per_test_temp_dir / "base"
        base_directory.mkdir()

        async with MultiDiskStore(base_directory=base_directory, auto_create=auto_create) as store:
            with pytest.raises(StoreSetupError, match="must resolve to a directory within"):
                await store.put("test_key", {"value": 1}, collection=collection)

            # A rejected name should not create cache files or affect valid collections.
            assert sorted(path.name for path in per_test_temp_dir.iterdir()) == ["base"]
            assert list(base_directory.iterdir()) == []
            (base_directory / "valid").mkdir()
            await store.put("test_key", {"value": 2}, collection="valid")
            assert await store.get("test_key", collection="valid") == {"value": 2}

    @pytest.mark.parametrize("target_name", ["outside", "missing", "base"])
    @pytest.mark.parametrize("auto_create", [True, False])
    async def test_reject_collection_symlink_outside_or_equal_to_base(self, per_test_temp_dir: Path, target_name: str, auto_create: bool):
        base_directory = per_test_temp_dir / "base"
        base_directory.mkdir()
        outside_directory = per_test_temp_dir / "outside"
        outside_directory.mkdir()
        link = base_directory / "linked"
        try:
            link.symlink_to(per_test_temp_dir / target_name, target_is_directory=True)
        except OSError as error:
            pytest.skip(f"Directory symlinks are unavailable: {error}")

        async with MultiDiskStore(base_directory=base_directory, auto_create=auto_create) as store:
            with pytest.raises(StoreSetupError, match="must resolve to a directory within"):
                await store.put("test_key", {"value": 1}, collection="linked")

        assert sorted(path.name for path in base_directory.iterdir()) == ["linked"]
        assert list(outside_directory.iterdir()) == []
        assert not (per_test_temp_dir / "missing").exists()

    async def test_preserve_symlink_to_directory_within_base(self, per_test_temp_dir: Path):
        base_directory = per_test_temp_dir / "base"
        target_directory = base_directory / "normal"
        target_directory.mkdir(parents=True)
        try:
            (base_directory / "linked").symlink_to(target_directory, target_is_directory=True)
        except OSError as error:
            pytest.skip(f"Directory symlinks are unavailable: {error}")

        async with MultiDiskStore(base_directory=base_directory, auto_create=False) as store:
            await store.put("test_key", {"value": 1}, collection="linked")
            assert await store.get("test_key", collection="linked") == {"value": 1}

        assert (target_directory / "cache.db").is_file()

    async def test_preserve_normal_collection_directory_on_reopen(self, per_test_temp_dir: Path):
        base_directory = per_test_temp_dir / "base"
        async with MultiDiskStore(base_directory=base_directory) as store:
            await store.put("test_key", {"value": 1}, collection="normal")

        assert (base_directory / "normal" / "cache.db").is_file()
        async with MultiDiskStore(base_directory=base_directory, auto_create=False) as store:
            assert await store.get("test_key", collection="normal") == {"value": 1}

    async def test_preserve_missing_directory_error_when_auto_create_disabled(self, per_test_temp_dir: Path):
        base_directory = per_test_temp_dir / "base"
        base_directory.mkdir()
        async with MultiDiskStore(base_directory=base_directory, auto_create=False) as store:
            with pytest.raises(StoreSetupError, match="does not exist"):
                await store.put("test_key", {"value": 1}, collection="normal")

        assert list(base_directory.iterdir()) == []

    async def test_custom_factory_still_controls_collection_directory(self, per_test_temp_dir: Path):
        cache = Cache(directory=str(per_test_temp_dir / "custom"))

        def factory(collection: str) -> Cache:
            assert collection == ".."
            return cache

        async with MultiDiskStore(disk_cache_factory=factory) as store:
            await store.put("test_key", {"value": 1}, collection="..")
            assert await store.get("test_key", collection="..") == {"value": 1}
