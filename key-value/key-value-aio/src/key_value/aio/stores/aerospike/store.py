from typing import Any, overload

from key_value.shared.errors import DeserializationError
from key_value.shared.type_checking.bear_spray import bear_spray
from key_value.shared.utils.compound import compound_key, get_keys_from_compound_keys
from key_value.shared.utils.managed_entry import ManagedEntry
from key_value.shared.utils.serialization import BasicSerializationAdapter, SerializationAdapter
from typing_extensions import override

from key_value.aio.stores.base import BaseContextManagerStore, BaseDestroyStore, BaseEnumerateKeysStore, BaseStore

try:
    import aerospike
except ImportError as e:
    msg = "AerospikeStore requires py-key-value-aio[aerospike]"
    raise ImportError(msg) from e

DEFAULT_NAMESPACE = "test"
DEFAULT_SET = "kv-store"
DEFAULT_PAGE_SIZE = 10000
PAGE_LIMIT = 10000


class AerospikeStore(BaseDestroyStore, BaseEnumerateKeysStore, BaseContextManagerStore, BaseStore):
    """Aerospike-based key-value store."""

    _client: aerospike.Client
    _namespace: str
    _set: str
    _adapter: SerializationAdapter

    @overload
    def __init__(
        self,
        *,
        client: aerospike.Client,
        namespace: str = DEFAULT_NAMESPACE,
        set_name: str = DEFAULT_SET,
        default_collection: str | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self,
        *,
        hosts: list[tuple[str, int]] | None = None,
        namespace: str = DEFAULT_NAMESPACE,
        set_name: str = DEFAULT_SET,
        default_collection: str | None = None,
    ) -> None: ...

    @bear_spray
    def __init__(
        self,
        *,
        client: aerospike.Client | None = None,
        hosts: list[tuple[str, int]] | None = None,
        namespace: str = DEFAULT_NAMESPACE,
        set_name: str = DEFAULT_SET,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the Aerospike store.

        Args:
            client: An existing Aerospike client to use.
            hosts: List of (host, port) tuples. Defaults to [("localhost", 3000)].
            namespace: Aerospike namespace. Defaults to "test".
            set_name: Aerospike set. Defaults to "kv-store".
            default_collection: The default collection to use if no collection is provided.
        """
        if client:
            self._client = client
        else:
            hosts = hosts or [("localhost", 3000)]
            config = {"hosts": hosts}
            self._client = aerospike.client(config)

        self._namespace = namespace
        self._set = set_name

        self._stable_api = True
        self._adapter = BasicSerializationAdapter(date_format="isoformat", value_format="dict")

        super().__init__(default_collection=default_collection)

    @override
    async def _setup(self) -> None:
        """Connect to Aerospike."""
        self._client.connect()

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        aerospike_key = (self._namespace, self._set, combo_key)

        try:
            (_key, _metadata, bins) = self._client.get(aerospike_key)
        except aerospike.exception.RecordNotFound:
            return None

        json_value: str | None = bins.get("value")

        if not isinstance(json_value, str):
            return None

        try:
            return self._adapter.load_json(json_str=json_value)
        except DeserializationError:
            return None

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        combo_key: str = compound_key(collection=collection, key=key)

        aerospike_key = (self._namespace, self._set, combo_key)
        json_value: str = self._adapter.dump_json(entry=managed_entry, key=key, collection=collection)

        bins = {"value": json_value}

        meta = {}
        if managed_entry.ttl is not None:
            # Aerospike TTL is in seconds
            meta["ttl"] = int(managed_entry.ttl)

        self._client.put(aerospike_key, bins, meta=meta)

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)

        aerospike_key = (self._namespace, self._set, combo_key)

        try:
            self._client.remove(aerospike_key)
        except aerospike.exception.RecordNotFound:
            return False
        else:
            return True

    @override
    async def _get_collection_keys(self, *, collection: str, limit: int | None = None) -> list[str]:
        limit = min(limit or DEFAULT_PAGE_SIZE, PAGE_LIMIT)

        pattern = compound_key(collection=collection, key="")

        keys: list[str] = []

        def callback(record: tuple[Any, Any, Any]) -> None:  # pyright: ignore[reportAny]
            (_namespace, _set, primary_key, _bins) = record  # pyright: ignore[reportAny]
            if isinstance(primary_key, str) and primary_key.startswith(pattern):
                keys.append(primary_key)

        # Scan the set for keys matching the collection
        scan = self._client.scan(self._namespace, self._set)
        scan.foreach(callback)

        # Extract just the key part from compound keys
        result_keys = get_keys_from_compound_keys(compound_keys=keys, collection=collection)

        return result_keys[:limit]

    @override
    async def _delete_store(self) -> bool:
        """Truncate the set (delete all records in the set)."""
        # Aerospike truncate requires a timestamp parameter
        # Using 0 means truncate everything
        self._client.truncate(self._namespace, self._set, 0)
        return True

    @override
    async def _close(self) -> None:
        """Close the Aerospike connection."""
        self._client.close()
