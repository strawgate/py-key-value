## AIO Code Review – Larger Findings and Recommendations

### Executive summary
- Elasticsearch: date handling and aggregation usage issues; potential under-counting and stale culling.
- MongoDB: implementation does not match documented design; async import path risk.
- Disk stores: inconsistent persistence of expiration metadata vs backend TTL.
- API consistency: `list[str]` vs `Sequence[str]` divergence across wrappers/stores.
- Performance: default bulk ops are sequential; consider native batch ops and concurrency.
- Constants duplication: `DEFAULT_COLLECTION_NAME` duplicated in wrappers.

---

### 1) Elasticsearch date handling (culling) uses seconds where ES expects ISO or epoch_millis
Problem: The cull query compares a `date` field against `now_as_epoch()` (seconds). ES date range comparisons should use an ISO string or epoch milliseconds.

Snippet (reference):

```23:27:/Users/bill.easton/repos/py-kv-store-adapter/key-value/key-value-aio/src/key_value/aio/stores/elasticsearch/store.py
        _ = await self._client.options(ignore_status=404).delete_by_query(
            index=f"{self._index_prefix}-*",
            body={
                "query": {
                    "range": {
                        "expires_at": {"lt": now_as_epoch()},
                    },
                },
            },
        )
```

Impact: Expired documents may not be culled reliably in clusters expecting epoch_millis.

Recommendation: Compare against ISO-8601 or epoch_millis, e.g. `now().isoformat()` or `int(now_as_epoch() * 1000)` with a matching mapping/format hint.

---

### 2) Elasticsearch aggregations/fields usage likely incorrect or incomplete
- Collection listing uses a `terms` aggregation without setting `size`, which defaults to 10 buckets.

Snippet (reference):

```72:89:/Users/bill.easton/repos/py-kv-store-adapter/key-value/key-value-aio/src/key_value/aio/stores/elasticsearch/store.py
        search_response: ObjectApiResponse[Any] = await self._client.options(ignore_status=404).search(
            index=f"{self._index_prefix}-*",
            aggregations={
                "collections": {
                    "terms": {
                        "field": "collection",
                    },
                },
            },
            size=limit,
        )
```

Impact: Only 10 unique collections will be returned regardless of `limit`.

Recommendation: Set `aggregations.terms.size = min(limit, PAGE_LIMIT)`.

- Key enumeration passes `fields=[{"key": None}]` and `source_includes=[]`. ES 8 expects `fields=["key"]` and either `"_source": false` or `"_source": {"includes": [...]}`.

Snippet (reference):

```39:51:/Users/bill.easton/repos/py-kv-store-adapter/key-value/key-value-aio/src/key_value/aio/stores/elasticsearch/store.py
        result: ObjectApiResponse[Any] = await self._client.options(ignore_status=404).search(
            index=self._sanitize_index_name(collection=collection),
            fields=[{"key": None}],
            body={
                "query": {
                    "term": {
                        "collection": collection,
                    },
                },
            },
            source_includes=[],
            size=limit,
        )
```

Impact: May return no `fields` and/or trigger parameter validation issues depending on client/server versions.

Recommendation: Use `fields=["key"]` and `"_source": false` (or the modern `_source` structure) to reduce payload.

---

### 3) MongoDB store design mismatch vs documentation; async import stability
- The docstring states a single backing collection using compound keys, but the code provisions per-collection collections and stores raw `key` (not compound).

Snippet (reference – documentation vs code paths):

```76:82:/Users/bill.easton/repos/py-kv-store-adapter/key-value/key-value-aio/src/key_value/aio/stores/mongodb/store.py
        The store uses a single MongoDB collection to persist entries for all adapter collections.
        We store compound keys "{collection}::{key}" and a JSON string payload. Optional TTL is persisted
        as ISO timestamps in the JSON payload itself to maintain consistent semantics across backends.
```

```112:127:/Users/bill.easton/repos/py-kv-store-adapter/key-value/key-value-aio/src/key_value/aio/stores/mongodb/store.py
    async def _setup_collection(self, *, collection: str) -> None:
        collection = self._sanitize_collection_name(collection=collection)
        matching_collections: list[str] = await self._db.list_collection_names(filter={"name": collection})
        if matching_collections:
            self._collections_by_name[collection] = self._db[collection]
            return
        new_collection: AsyncCollection[dict[str, Any]] = await self._db.create_collection(name=collection)
        _ = await new_collection.create_index(keys="key")
        self._collections_by_name[collection] = new_collection
```

Impact: Behavior diverges from stated contract and from backends that rely on compound keys in a single collection.

Recommendation: Decide on one of:
- Align implementation to the docstring: use a single physical collection, key as `"{collection}::{key}"`, and index `key`.
- Or update the documentation to specify per-collection collections and ensure key naming, indexing, and cleanup semantics are consistent.

- Async imports: the package path `pymongo.asynchronous` may vary across PyMongo versions; ensure compatibility with the installed major version (PyMongo 5 uses the asyncio client under `pymongo` with different import paths). Consider isolating imports behind a small compatibility shim.

---

### 4) Disk-backed stores: inconsistent use of expiration metadata
- `DiskStore` writes with metadata (`to_json()`), while `MultiDiskStore` writes without expiration metadata (`include_expiration=False`). Both rely on the backend’s TTL for actual expiry.

Snippets (reference):

```100:108:/Users/bill.easton/repos/py-kv-store-adapter/key-value/key-value-aio/src/key_value/aio/stores/disk/store.py
        _ = self._cache.set(key=combo_key, value=managed_entry.to_json(), expire=managed_entry.ttl)
```

```132:135:/Users/bill.easton/repos/py-kv-store-adapter/key-value/key-value-aio/src/key_value/aio/stores/disk/multi_store.py
        _ = self._cache[collection].set(key=combo_key, value=managed_entry.to_json(include_expiration=False), expire=managed_entry.ttl)
```

Impact: Mixed on-disk payload formats; may confuse downstream tools or future migrations. `DiskStore` also stores an `expires_at` alongside relying on the cache’s own TTL, which can drift over time.

Recommendation: Standardize on payload format (with or without expiration metadata). If the backend TTL is authoritative, prefer omitting `expires_at` in stored JSON for consistency.

---

### 5) API consistency: `keys` parameters as `list[str]` vs `Sequence[str]`
- The protocol now specifies `list[str]` for bulk `keys`. Several wrappers still type them as `Sequence[str]`.

Snippets (reference – protocol):

```60:73:/Users/bill.easton/repos/py-kv-store-adapter/key-value/key-value-aio/src/key_value/aio/protocols/key_value.py
    async def get_many(self, keys: list[str], *, collection: str | None = None) -> list[dict[str, Any] | None]:
    ...
    async def ttl_many(self, keys: list[str], *, collection: str | None = None) -> list[tuple[dict[str, Any] | None, float | None]]:
    ...
    async def put_many(self, keys: list[str], values: Sequence[dict[str, Any]], *, collection: str | None = None, ttl: Sequence[float | None] | float | None = None) -> None:
    ...
    async def delete_many(self, keys: list[str], *, collection: str | None = None) -> int:
```

Impact: Mixed method signatures can cause type-checking friction and confusion.

Recommendation: Normalize wrapper/store method signatures to accept `list[str]` for `keys` to match the protocol.

---

### 6) Performance: default bulk operations are sequential
- Base fallbacks fetch and delete entries one-by-one with awaited calls in a loop.

Snippets (reference):

```98:101:/Users/bill.easton/repos/py-kv-store-adapter/key-value/key-value-aio/src/key_value/aio/stores/base.py
    async def _get_managed_entries(...):
        return [await self._get_managed_entry(..., key=key) for key in keys]
```

```234:243:/Users/bill.easton/repos/py-kv-store-adapter/key-value/key-value-aio/src/key_value/aio/stores/base.py
    async def _delete_managed_entries(...):
        for key in keys:
            if await self._delete_managed_entry(..., key=key):
                deleted_count += 1
```

Impact: Increased latency and load for backends that support native batch operations.

Recommendation: Override bulk methods in backends where feasible (e.g., Redis `MGET`/`PIPELINE`, MongoDB bulk operations, Elasticsearch multi-get). Where not feasible, consider `asyncio.gather` with an upper bound on concurrency.

---

### 7) Constants duplication: `DEFAULT_COLLECTION_NAME`
- `StatisticsWrapper` defines its own `DEFAULT_COLLECTION_NAME` rather than importing it, while other wrappers import it from the base store.

Snippets (reference):

```97:104:/Users/bill.easton/repos/py-kv-store-adapter/key-value/key-value-aio/src/key_value/aio/wrappers/statistics/wrapper.py
DEFAULT_COLLECTION_NAME = "default_collection"
class StatisticsWrapper(BaseWrapper):
```

Impact: Drift risk if the default name changes in the base store.

Recommendation: Import `DEFAULT_COLLECTION_NAME` from `key_value.aio.stores.base`.

---

### Appendix: items already addressed with simple edits
- Accept int-like TTLs consistently and clamp correctly.
- Fixed misuse of `enumerate(iterable=...)` in the passthrough cache wrapper.
- Hardened Elasticsearch utils casting.
- MongoDB collection setup now maps existing collections and closes the client.
- Clarified DiskStore parameter error message.

