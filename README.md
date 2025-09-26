# KV Store Adapter

A pluggable, async-only key-value store interface for modern Python applications.

## Features

- **Async-only**: Built from the ground up with `async`/`await` support
- **Multiple backends**: Elasticsearch, Memcached, MongoDB, Redis, Valkey, and In-memory, Disk, etc
- **TTL support**: Automatic expiration handling across all store types
- **Type-safe**: Full type hints with Protocol-based interfaces
- **Adapters**: Pydantic model support, raise-on-missing behavior, etc
- **Wrappers**: Statistics tracking and extensible wrapper system
- **Collection-based**: Organize keys into logical collections/namespaces
- **Pluggable architecture**: Easy to add custom store implementations

## Quick Start

```bash
pip install kv-store-adapter

# With specific backend support
pip install kv-store-adapter[redis]
pip install kv-store-adapter[elasticsearch]
pip install kv-store-adapter[memory]
pip install kv-store-adapter[disk]
pip install kv-store-adapter[memcached]
pip install kv-store-adapter[mongodb]
pip install kv-store-adapter[valkey]

# With all backends
pip install kv-store-adapter[memory,disk,redis,elasticsearch,memcached,mongodb,valkey]

# With Pydantic adapter support
pip install kv-store-adapter[pydantic]
```

# The KV Store Protocol

The simplest way to get started is to use the `KVStore` interface, which allows you to write code that works with any supported KV Store:

```python
import asyncio

from kv_store_adapter.types import KVStore
from kv_store_adapter.stores.redis.store import RedisStore
from kv_store_adapter.stores.memory.store import MemoryStore

async def example():
    # In-memory store
    memory_store = MemoryStore()
    await memory_store.put(key="456", value={"name": "Bob"}, collection="users", ttl=3600) # TTL is supported, but optional!
    bob = await memory_store.get(key="456", collection="users")
    await memory_store.delete(key="456", collection="users")

    redis_store = RedisStore(url="redis://localhost:6379")
    await redis_store.put(key="123", value={"name": "Alice"}, collection="products")
    alice = await redis_store.get(key="123", collection="products")
    await redis_store.delete(key="123", collection="products")

asyncio.run(example())
```

## Store Implementations

Choose the store that best fits your needs. All stores implement the same `KVStore` interface:

### Production Stores

- **ElasticsearchStore**: `ElasticsearchStore(url="https://localhost:9200", api_key="your-api-key")`
- **RedisStore**: `RedisStore(url="redis://localhost:6379/0")`
- **MongoDBStore**: `MongoDBStore(url="mongodb://localhost:27017/test")`
- **ValkeyStore**: `ValkeyStore(host="localhost", port=6379)`
- **MemcachedStore**: `MemcachedStore(host="localhost", port=11211)`
- **DiskStore**: A disk-based store using diskcache `DiskStore(directory="./cache")`. Also see `MultiDiskStore` for a store that creates one disk store per collection.
- **MemoryStore**: A fast in-memory TLRU cache `MemoryStore()`

### Development/Testing Stores  

- **SimpleStore**: In-memory and inspectable for testing `SimpleStore()`
- **NullStore**: No-op store for testing `NullStore()`

For detailed configuration options and all available stores, see [DEVELOPING.md](DEVELOPING.md).

## Atomicity / Consistency

We strive to support atomicity and consistency across basic key-value operations across all stores and operations in the KVStore. That being said, each store may have different guarantees for consistency and atomicity. Especially with distributed stores like MongoDB, Redis, etc and especially with bulk/management operations.

## Protocol Adapters

The library provides an adapter pattern simplifying the use of the protocol/store. Adapters themselves do not implement the `KVStore` interface and cannot be nested. As a result, Adapters are the "outer" layer of the store. Adapters are primarily for improved type-safe operations.

The following adapters are available:

- **PydanticAdapter**: Type-safe storage and retrieval using Pydantic models with automatic serialization/deserialization.
- **RaiseOnMissingAdapter**: Provides optional raise-on-missing behavior for get, get_many, ttl, and ttl_many operations.

For example, the PydanticAdapter can be used to provide type-safe interactions with a store:

```python
from pydantic import BaseModel

from kv_store_adapter.adapters.pydantic import PydanticAdapter
from kv_store_adapter.stores.memory.store import MemoryStore

class User(BaseModel):
    name: str
    email: str

memory_store = MemoryStore()

user_adapter = PydanticAdapter(kv_store=memory_store, pydantic_model=User)

async def example():
    await user_adapter.put(key="123", value=User(name="John Doe", email="john.doe@example.com"), collection="users")
    user: User | None = await user_adapter.get(key="123", collection="users")

asyncio.run(example())
```

## Wrappers

The library provides a wrapper pattern for adding functionality to a store. Wrappers themselves implement the `KVStore` interface meaning that you can wrap any store with any wrapper, and chain wrappers together as needed.

### Statistics Tracking

Track operation statistics for any store:

```python
import asyncio

from kv_store_adapter.wrappers.statistics import StatisticsWrapper
from kv_store_adapter.stores.memory.store import MemoryStore

memory_store = MemoryStore()
store = StatisticsWrapper(store=memory_store)

async def example():
    # Use store normally - statistics are tracked automatically
    await store.put(key="123", value={"name": "Alice"}, collection="users")
    await store.get(key="123", collection="users")
    await store.get(key="456", collection="users")  # Cache miss

    # Access statistics
    stats = store.statistics
    user_stats = stats.get_collection("users")
    print(f"Total gets: {user_stats.get.count}")
    print(f"Cache hits: {user_stats.get.hit}")
    print(f"Cache misses: {user_stats.get.miss}")

asyncio.run(example())
```

Other wrappers that are available include:

- **ClampTTLWrapper**: Wraps a store and clamps the TTL to a given range.
- **TTLClampWrapper**: Wraps a store and clamps the TTL to a given range.
- **PassthroughCacheWrapper**: Wraps two stores to provide a read-through cache. Reads go to the cache store first and fall back to the primary store, populating the cache with the entry from the primary; writes evict from the cache and then write to the primary. For example, use a RedisStore as the primary and a MemoryStore as the cache store. Or a DiskStore as the primary and a MemoryStore as the cache store.
- **PrefixCollectionsWrapper**: Wraps a store and prefixes all collections with a given prefix.
- **PrefixKeysWrapper**: Wraps a store and prefixes all keys with a given prefix.
- **SingleCollectionWrapper**: Wraps a store and forces all requests into a single collection.
- **StatisticsWrapper**: Wraps a store and tracks hit/miss statistics for the store.

See [DEVELOPING.md](DEVELOPING.md) for more information on how to create your own wrappers.

## Chaining Wrappers, Adapters, and Stores

Imagine you have a service where you want to cache 3 pydantic models in a single collection. You can do this by wrapping the store in a PydanticAdapter and a SingleCollectionWrapper:

```python
import asyncio

from kv_store_adapter.adapters.pydantic import PydanticAdapter
from kv_store_adapter.wrappers.single_collection import SingleCollectionWrapper
from kv_store_adapter.stores.memory.store import MemoryStore
from pydantic import BaseModel

class User(BaseModel):
    name: str
    email: str

store = MemoryStore()

users_store = PydanticAdapter(kv_store=SingleCollectionWrapper(store=store, single_collection="users", default_collection="default"), pydantic_model=User)
products_store = PydanticAdapter(kv_store=SingleCollectionWrapper(store=store, single_collection="products", default_collection="default"), pydantic_model=Product)
orders_store = PydanticAdapter(kv_store=SingleCollectionWrapper(store=store, single_collection="orders", default_collection="default"), pydantic_model=Order)

async def example():
    new_user: User = User(name="John Doe", email="john.doe@example.com")
    await users_store.put(key="123", value=new_user, collection="allowed_users")

    john_doe: User | None = await users_store.get(key="123", collection="allowed_users")

asyncio.run(example())
```

The SingleCollectionWrapper will result in writes to the `allowed_users` collection being redirected to the `users` collection but the keys will be prefixed with the original collection `allowed_users__` name. So the key `123` will be stored as `allowed_users__123` in the `users` collection.

Note: The above example shows the conceptual usage, but you would need to define `Product` and `Order` models as well for the complete example to work.

## Development

See [DEVELOPING.md](DEVELOPING.md) for development setup, testing, and contribution guidelines.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please read [DEVELOPING.md](DEVELOPING.md) for development setup and contribution guidelines.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and changes.
