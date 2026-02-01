# Python Key-Value Libraries

This monorepo contains:

- `py-key-value-aio`: Async key-value store library (primary and supported).
- `py-key-value-sync`: Sync version (no longer planned; async-only focus).

## Documentation

- [Full Documentation](https://strawgate.com/py-key-value/)
- [Getting Started Guide](https://strawgate.com/py-key-value/getting-started/)
- [Stores Guide](https://strawgate.com/py-key-value/stores/)
- [Wrappers Guide](https://strawgate.com/py-key-value/wrappers/)
- [Adapters Guide](https://strawgate.com/py-key-value/adapters/)
- [API Reference](https://strawgate.com/py-key-value/api/protocols/)

## Why use this library?

- **Multiple backends**: Aerospike, DynamoDB, S3, Elasticsearch, Firestore, Memcached,
  MongoDB, Redis, RocksDB, Valkey, and In-memory, Disk, etc.
- **TTL support**: Automatic expiration handling across all store types
- **Type-safe**: Full type hints with Protocol-based interfaces
- **Adapters**: Pydantic model support, raise-on-missing behavior, etc
- **Wrappers**: Statistics tracking and extensible wrapper system
- **Collection-based**: Organize keys into logical collections/namespaces
- **Pluggable architecture**: Easy to add custom store implementations

## Value to Framework Authors

While key-value storage is valuable for individual projects, its true power
emerges when framework authors use it as a **pluggable abstraction layer**.

By coding your framework against the `AsyncKeyValue` protocol, you enable your
users to choose their own storage backend without changing a single line of
your framework code. Users can seamlessly switch between local caching
(memory, disk) for development and distributed storage (Redis, DynamoDB,
MongoDB) for production.

### Real-World Example: FastMCP

[FastMCP](https://github.com/jlowin/fastmcp) demonstrates this pattern
perfectly. FastMCP framework authors use the `AsyncKeyValue` protocol for:

- **Response caching middleware**: Store and retrieve cached responses
- **OAuth proxy tokens**: Persist authentication tokens across sessions

FastMCP users can plug in any store implementation:

- Development: `MemoryStore()` for fast iteration
- Production: `RedisStore()` for distributed caching
- Testing: `NullStore()` for testing without side effects

### How to Use This in Your Framework

1. **Accept the protocol** in your framework's initialization:

   ```python
   from key_value.aio.protocols.key_value import AsyncKeyValue

   class YourFramework:
       def __init__(self, cache: AsyncKeyValue):
           self.cache = cache
   ```

2. **Use simple key-value operations** in your framework:

   ```python
   # Store data
   await self.cache.put(
       key="session:123",
       value={"user_id": "456", "expires": "2024-01-01"},
       collection="sessions",
       ttl=3600
   )

   # Retrieve data
   session = await self.cache.get(key="session:123", collection="sessions")
   ```

3. **Let users choose their backend**:

   ```python
   # User's code - they control the storage backend
   from your_framework import YourFramework
   from key_value.aio.stores.redis import RedisStore
   from key_value.aio.stores.memory import MemoryStore

   # Development
   framework = YourFramework(cache=MemoryStore())

   # Production
   framework = YourFramework(
       cache=RedisStore(url="redis://localhost:6379/0")
   )
   ```

By depending on `py-key-value-aio` instead of a specific storage backend,
you give your users the flexibility to choose the right storage for their
needs while keeping your framework code clean and backend-agnostic.

## Why not use this library?

- **Async-only**: This library focuses exclusively on async/await patterns.
  A synchronous wrapper library is not currently planned.
- **Managed Entries**: Raw values are not stored in backends, a wrapper object
  is stored instead. This wrapper object contains the value, sometimes metadata
  like the TTL, and the creation timestamp. Most often it is serialized to and
  from JSON.
- **No Live Objects**: Even when using the in-memory store, "live" objects are
  never returned from the store. You get a dictionary or a Pydantic model,
  hopefully a copy of what you stored, but never the same instance in memory.
- **Dislike of Bear Bros**: Beartype is used for runtime type checking. Core
  protocol methods in store and wrapper implementations (put/get/delete/ttl
  and their batch variants) enforce types and will raise TypeError for
  violations. Other code produces warnings. You can disable all beartype
  checks by setting `PY_KEY_VALUE_DISABLE_BEARTYPE=true` or suppress warnings
  via the warnings module.

## Installation

## Quick start for Async library

Install the library with the backends you need.

```bash
# Async library
pip install py-key-value-aio

# With specific backend extras
pip install py-key-value-aio[memory]
pip install py-key-value-aio[disk]
pip install py-key-value-aio[dynamodb]
pip install py-key-value-aio[s3]
pip install py-key-value-aio[elasticsearch]
pip install py-key-value-aio[firestore]
# or: aerospike, redis, mongodb, memcached, valkey, vault, registry, rocksdb, see below for all options
```

```python
import asyncio

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.stores.memory import MemoryStore


async def example(key_value: AsyncKeyValue) -> None:
    await key_value.put(key="123", value={"name": "Alice"}, collection="users", ttl=3600)
    value = await key_value.get(key="123", collection="users")
    await key_value.delete(key="123", collection="users")


async def main():
    memory_store = MemoryStore()
    await example(key_value=memory_store)

asyncio.run(main())
```

## Introduction to py-key-value

### Protocols

- **Async**: `key_value.aio.protocols.AsyncKeyValue` — async
  `get/put/delete/ttl` and bulk variants; optional protocol segments for
  culling, destroying stores/collections, and enumerating keys/collections
  implemented by capable stores.

The protocols offer a simple interface for your application to interact with
the store:

```python
get(key: str, collection: str | None = None) -> dict[str, Any] | None:
get_many(keys: list[str], collection: str | None = None) -> list[dict[str, Any] | None]:

put(key: str, value: dict[str, Any], collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
put_many(keys: list[str], values: Sequence[dict[str, Any]], collection: str | None = None, ttl: SupportsFloat | None = None) -> None:

delete(key: str, collection: str | None = None) -> bool:
delete_many(keys: list[str], collection: str | None = None) -> int:

ttl(key: str, collection: str | None = None) -> tuple[dict[str, Any] | None, float | None]:
ttl_many(keys: list[str], collection: str | None = None) -> list[tuple[dict[str, Any] | None, float | None]]:
```

### Stores

The library provides multiple store implementations organized into three
categories:

- **Local stores**: In-memory and disk-based storage (Memory, Disk, RocksDB, etc.)
- **Secret stores**: Secure OS-level storage for sensitive data (Keyring, Vault)
- **Distributed stores**: Network-based storage for multi-node apps (Redis,
  DynamoDB, S3, MongoDB, etc.)

Each store has a **stability rating** indicating likelihood of
backwards-incompatible changes. Stable stores (Redis, Valkey, Disk, Keyring)
are recommended for long-term storage.

**[📚 View all stores, installation guides, and examples →](https://strawgate.com/py-key-value/stores/)**

### Adapters

Adapters provide specialized interfaces for working with stores. Unlike wrappers,
they don't implement the protocol but instead offer alternative APIs for specific
use cases:

- **DataclassAdapter**: Type-safe dataclass storage with automatic validation
- **PydanticAdapter**: Type-safe Pydantic model storage with serialization
- **RaiseOnMissingAdapter**: Raise exceptions instead of returning None for
  missing keys

**[📚 View all adapters with examples →](https://strawgate.com/py-key-value/adapters/)**

**Quick example** - PydanticAdapter for type-safe storage:

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

Wrappers add functionality to stores while implementing the protocol themselves,
allowing them to be stacked and used anywhere a store is expected. Available
wrappers include:

- **Performance**: Compression, Caching (Passthrough), Statistics, Timeout
- **Security**: Encryption (Fernet), ReadOnly
- **Reliability**: Retry, Fallback
- **Routing**: CollectionRouting, Routing, SingleCollection
- **Organization**: PrefixKeys, PrefixCollections
- **Constraints**: LimitSize, TTLClamp, DefaultValue
- **Observability**: Logging, Statistics

**[📚 View all wrappers with examples →](https://strawgate.com/py-key-value/wrappers/)**

Wrappers can be stacked for complex functionality:

```python
# Create a retriable redis store with timeout protection that is monitored,
# with compressed values, and a fallback to memory store! This probably isn't
# a good idea but you can do it!
store = 
LoggingWrapper(
    CompressionWrapper(
        FallbackWrapper(
            primary_key_value=RetryWrapper(
                TimeoutWrapper(
                    key_value=redis_store,
                )
            ),
            fallback_key_value=memory_store,
        )
    )
)
```

Wrappers are applied in order, so the outermost wrapper is applied first and
the innermost wrapper is applied last. Keep this in mind when chaining
wrappers!

### Atomicity / Consistency

We aim for consistent semantics across basic key-value operations. Guarantees
may vary by backend (especially distributed systems) and for bulk or management
operations.

## Advanced Patterns

Adapters, stores, and wrappers can be combined in a variety of ways as needed.

The following example simulates a consumer of your service providing an
Elasticsearch store and forcing all data into a single collection. They pass
this wrapped store to your service and you further wrap it in a statistics
wrapper (for metrics/monitoring) and a pydantic adapter, to simplify the
application's usage.

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

elasticsearch_store: ElasticsearchStore = ElasticsearchStore(
    url="https://localhost:9200", api_key="your-api-key", index="kv-store"
)

single_collection: SingleCollectionWrapper = SingleCollectionWrapper(
    key_value=elasticsearch_store, single_collection="users",
    default_collection="one-collection"
)


async def main(key_value: AsyncKeyValue):
    statistics_wrapper = StatisticsWrapper(key_value=key_value)
    users = PydanticAdapter(key_value=statistics_wrapper, pydantic_model=User)

    await users.put(
        key="u1", value=User(name="Jane", email="j@example.com"),
        collection="ignored"
    )
    user = await users.get(key="u1", collection="ignored")
    _ = statistics_wrapper.statistics  # access metrics


asyncio.run(main(key_value=single_collection))
```

## Project links

- [Full Documentation](https://strawgate.com/py-key-value/)
- [API Reference](https://strawgate.com/py-key-value/api/protocols/)

Contributions welcome but may not be accepted. File an issue before submitting
a pull request. If you do not get agreement on your proposal before making a
pull request you may have a bad time.

Apache 2.0 licensed.
