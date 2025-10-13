# Python Key-Value Libraries

This monorepo contains two libraries:

- `py-key-value-aio`: Async key-value store library (supported).
- `py-key-value-sync`: Sync key-value store library (under development; generated from the async API).

## Why use this library?

- **Multiple backends**: Elasticsearch, Memcached, MongoDB, Redis, Valkey, and In-memory, Disk, etc
- **TTL support**: Automatic expiration handling across all store types
- **Type-safe**: Full type hints with Protocol-based interfaces
- **Adapters**: Pydantic model support, raise-on-missing behavior, etc
- **Wrappers**: Statistics tracking and extensible wrapper system
- **Collection-based**: Organize keys into logical collections/namespaces
- **Pluggable architecture**: Easy to add custom store implementations

## Why not use this library?

- **Async-only**: While a code-gen'd synchronous library is under development, the async library is the primary focus at the moment.
- **Managed Entries**: Raw values are not stored in backends, a wrapper object is stored instead. This wrapper object contains the value, sometimes metadata like the TTL, and the creation timestamp. Most often it is serialized to and from JSON.
- **No Live Objects**: Even when using the in-memory store, "live" objects are never returned from the store. You get a dictionary or a Pydantic model, hopefully a copy of what you stored, but never the same instance in memory.
- **Dislike of Bear Bros**: Beartype is used for runtime type checking, it will report warnings if you get too cheeky with what you're passing around. If you are not a fan of beartype, you can disable it by setting the `PY_KEY_VALUE_DISABLE_BEARTYPE` environment variable to `true` or you can disable the warnings via the warn module.

## Installation

## Quick start for Async library

Install the library with the backends you need.

```bash
# Async library
pip install py-key-value-aio

# With specific backend extras
pip install py-key-value-aio[memory]
pip install py-key-value-aio[disk]
pip install py-key-value-aio[elasticsearch]
# or: redis, mongodb, memcached, valkey, see below for all options
```

```python
import asyncio

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.stores.memory import MemoryStore


async def example(key_value: AsyncKeyValue) -> None:
    await store.put(key="123", value={"name": "Alice"}, collection="users", ttl=3600)
    value = await store.get(key="123", collection="users")
    await store.delete(key="123", collection="users")


async def main():
    memory_store = MemoryStore()
    await example(memory_store)

asyncio.run(main())
```

## Introduction to py-key-value

### Protocols

- **Async**: `key_value.aio.protocols.AsyncKeyValue` — async `get/put/delete/ttl` and bulk variants; optional protocol segments for culling, destroying stores/collections, and enumerating keys/collections implemented by capable stores.
- **Sync**: `key_value.sync.protocols.KeyValue` — sync mirror of the async protocol, generated from the async library.

The protocols offer a simple interface for your application to interact with the store:

```python
get(key: str, collection: str | None = None) -> dict[str, Any] | None:
get_many(keys: list[str], collection: str | None = None) -> list[dict[str, Any] | None]:

put(key: str, value: dict[str, Any], collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
put_many(keys: list[str], values: Sequence[dict[str, Any]], collection: str | None = None, ttl: Sequence[SupportsFloat | None] | SupportsFloat | None = None) -> None:

delete(key: str, collection: str | None = None) -> bool:
delete_many(keys: list[str], collection: str | None = None) -> int:

ttl(key: str, collection: str | None = None) -> tuple[dict[str, Any] | None, float | None]:
ttl_many(keys: list[str], collection: str | None = None) -> list[tuple[dict[str, Any] | None, float | None]]:
```

### Stores

The library provides a variety of stores that implement the protocol:

| Local Stores     | Async | Sync | Example |
|------------------|:-----:|:----:|:-------|
| Memory           |  ✅  |  ✅  | `MemoryStore()` |
| Disk             |  ✅  |  ✅  | `DiskStore(directory="./cache")` |
| Disk (Per-Collection) |  ✅  |  ✅  | `MultiDiskStore(directory="./cache")` |
| Simple (test)    |  ✅  |  ✅  | `SimpleStore()` |
| Null (test)      |  ✅  |  ✅  | `NullStore()` |

| Distributed Stores | Async | Sync | Example |
|------------------|:-----:|:----:|:-------|
| Elasticsearch    |  ✅  |  ✅  | `ElasticsearchStore(url="https://localhost:9200", api_key="your-api-key", index="kv-store")` |
| Memcached        |  ✅  |      | `MemcachedStore(host="127.0.0.1", port=11211")` |
| MongoDB          |  ✅  |  ✅  | `MongoDBStore(url="mongodb://localhost:27017/test")` |
| Redis            |  ✅  |  ✅  | `RedisStore(url="redis://localhost:6379/0")` |
| Valkey           |  ✅  |  ✅  | `ValkeyStore(host="localhost", port=6379)` |


### Adapters

Adapters "wrap" any protocol-compliant store but do not themselves implement the protocol.

They simplify your applications interactions with stores and provide additional functionality. While your application will accept an instance that implements the protocol, your application code might be simplified by using an adapter.

| Adapter | Description | Example |
|---------|-------------|---------|
| PydanticAdapter | Type-safe storage/retrieval of Pydantic models with transparent serialization/deserialization. | `PydanticAdapter(key_value=memory_store, pydantic_model=User)` |
| RaiseOnMissingAdapter | Optional raise-on-missing behavior for `get`, `get_many`, `ttl`, and `ttl_many`. | `RaiseOnMissingAdapter(key_value=memory_store)` |

For example, the PydanticAdapter allows you to store and retrieve Pydantic models with transparent serialization/deserialization:

```python
import asyncio
from pydantic import BaseModel

from key_value.aio.adapters.pydantic import PydanticAdapter
from key_value.aio.stores.memory import MemoryStore

class User(BaseModel):
    name: str
    email: str

async def example():
    memory_store: MemoryStore = MemoryStore()

    user_adapter: PydanticAdapter[User] = PydanticAdapter(
        key_value=memory_store,
        pydantic_model=User,
        default_collection="users",
    )

    new_user: User = User(name="John Doe", email="john.doe@example.com")
    
    # Directly store the User model
    await user_adapter.put(
        key="john-doe",
        value=new_user,
    )

    # Retrieve the User model
    existing_user: User | None = await user_adapter.get(
        key="john-doe",
    )

asyncio.run(example())
```

### Wrappers

The library provides a wrapper pattern for adding functionality to a store. Wrappers themselves implement the protocol meaning that you can wrap any store with any wrapper, and chain wrappers together as needed.

The following wrappers are available:

| Wrapper | Description | Example |
|---------|---------------|-----|
| StatisticsWrapper | Track operation statistics for the store. | `StatisticsWrapper(key_value=memory_store)` |
| TTLClampWrapper | Clamp the TTL to a given range. | `TTLClampWrapper(key_value=memory_store, min_ttl=60, max_ttl=3600)` |
| PassthroughCacheWrapper | Wrap two stores to provide a read-through cache. | `PassthroughCacheWrapper(primary_key_value=memory_store, cache_key_value=memory_store)` |
| PrefixCollectionsWrapper | Prefix all collections with a given prefix. | `PrefixCollectionsWrapper(key_value=memory_store, prefix="users")` |
| PrefixKeysWrapper | Prefix all keys with a given prefix. | `PrefixKeysWrapper(key_value=memory_store, prefix="users")` |

### Atomicity / Consistency

We aim for consistent semantics across basic key-value operations. Guarantees may vary by backend (especially distributed systems) and for bulk or management operations.


## Advanced Patterns

Adapters, stores, and wrappers can be combined in a variety of ways as needed.

The following example simulates a consumer of your service providing an Elasticsearch store and forcing all data into a single collection. They pass this wrapped store to your service and you further wrap it in a statistics wrapper (for metrics/monitoring) and a pydantic adapter, to simplify the application's usage.

```python
import asyncio
from pydantic import BaseModel

from key_value.aio.adapters.pydantic import PydanticAdapter
from key_value.aio.wrappers.single_collection import SingleCollectionWrapper
from key_value.aio.wrappers.statistics import StatisticsWrapper
from key_value.aio.stores.elasticsearch import ElasticsearchStore


class User(BaseModel):
    name: str
    email: str

elasticsearch_store: ElasticsearchStore = ElasticsearchStore(url="https://localhost:9200", api_key="your-api-key", index="kv-store")

single_collection: SingleCollectionWrapper = SingleCollectionWrapper(key_value=elasticsearch_store, single_collection="users", default_collection="one-collection")


async def main(key_value: AsyncKeyValue):
    statistics_wrapper = StatisticsWrapper(key_value=elasticsearch_store)
    users = PydanticAdapter(key_value=statistics_wrapper, pydantic_model=User)

    await users.put(key="u1", value=User(name="Jane", email="j@example.com"), collection="ignored")
    user = await users.get(key="u1", collection="ignored")
    _ = statistics_wrapper.statistics  # access metrics


asyncio.run(main(key_value=single_collection))
```

## Sync library status

The sync library is under development and mirrors the async library. The goal is to code gen the vast majority of the syncronous library from the async library.

## Project links

- Async README: `key-value/key-value-aio/README.md`
- Sync README: `key-value/key-value-sync/README.md`

Contributions welcome but may not be accepted. File an issue before submitting a pull request. If you do not get agreement on your proposal before making a pull request you may have a bad time.

MIT licensed.