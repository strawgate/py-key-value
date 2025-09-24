# KV Store Adapter

A pluggable, async-first key-value store interface for Python applications with support for multiple backends and TTL (Time To Live) functionality.

## Features

- **Async-first**: Built from the ground up with `async`/`await` support
- **Multiple backends**: Redis, Elasticsearch, In-memory, Disk, and more
- **TTL support**: Automatic expiration handling across all store types
- **Type-safe**: Full type hints with Protocol-based interfaces
- **Adapters**: Pydantic, Single Collection, and more
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

# With all backends
pip install kv-store-adapter[memory,disk,redis,elasticsearch]
```

# The KV Store Protocol

The simplest way to get started is to use the `KVStoreProtocol` interface, which allows you to write code that works with any supported KV Store:

```python
import asyncio

from kv_store_adapter.types import KVStoreProtocol
from kv_store_adapter.stores.redis import RedisStore
from kv_store_adapter.stores.memory import MemoryStore

async def example():
    # In-memory store
    memory_store = MemoryStore()
    await memory_store.put(collection="users", key="456", value={"name": "Bob"}, ttl=3600) # TTL is supported, but optional!
    bob = await memory_store.get(collection="users", key="456")
    await memory_store.delete(collection="users", key="456")

    redis_store = RedisStore(url="redis://localhost:6379")
    await redis_store.put(collection="products", key="123", value={"name": "Alice"})
    alice = await redis_store.get(collection="products", key="123")
    await redis_store.delete(collection="products", key="123")

asyncio.run(example())
```

## Store Implementations

Choose the store that best fits your needs. All stores implement the same `KVStoreProtocol` interface:

### Production Stores

- **RedisStore**: `RedisStore(url="redis://localhost:6379/0")`
- **ElasticsearchStore**: `ElasticsearchStore(url="https://localhost:9200", api_key="your-api-key")`
- **DiskStore**: A sqlite-based store for local persistence `DiskStore(path="./cache")`
- **MemoryStore**: A fast in-memory cache `MemoryStore()`

### Development/Testing Stores  

- **SimpleStore**: In-memory and inspectable for testing `SimpleStore()`
- **NullStore**: No-op store for testing `NullStore()`

For detailed configuration options and all available stores, see [DEVELOPING.md](DEVELOPING.md).

## Atomicity / Consistency

We strive to support atomicity and consistency across all stores and operations in the KVStoreProtocol. That being said,
there are operations available via the BaseKVStore class which are management operations like listing keys, listing collections, clearing collections, culling expired entries, etc. These operations may not be atomic, may be eventually consistent across stores, or may have other limitations (like limited to returning a certain number of keys).

## Adapters

The library provides an adapter pattern simplifying the user of the protocol/store. Adapters themselves do not implement the `KVStoreProtocol` interface and cannot be nested. Adapters can be used with wrappers and stores interchangeably.

The following adapters are available:

- **PydanticAdapter**: Converts data to and from a store using Pydantic models.
- **SingleCollectionAdapter**: Provides KV operations that do not require a collection parameter.

For example, the PydanticAdapter can be used to provide type-safe interactions with a store:

```python
from pydantic import BaseModel

from kv_store_adapter.adapters.pydantic import PydanticAdapter
from kv_store_adapter.stores.memory import MemoryStore

class User(BaseModel):
    name: str
    email: str

memory_store = MemoryStore()

user_adapter = PydanticAdapter(store=memory_store, pydantic_model=User)

async def example():
    await user_adapter.put(collection="users", key="123", value=User(name="John Doe", email="john.doe@example.com"))
    user: User | None = await user_adapter.get(collection="users", key="123")

asyncio.run(example())
```

## Wrappers

The library provides a wrapper pattern for adding functionality to a store. Wrappers themselves implement the `KVStoreProtocol` interface meaning that you can wrap any
store with any wrapper, and chain wrappers together as needed.

### Statistics Tracking

Track operation statistics for any store:

```python
import asyncio

from kv_store_adapter.stores.wrappers.statistics import StatisticsWrapper
from kv_store_adapter.stores.memory import MemoryStore

memory_store = MemoryStore()
store = StatisticsWrapper(store=memory_store)

async def example():
    # Use store normally - statistics are tracked automatically
    await store.put("users", "123", {"name": "Alice"})
    await store.get("users", "123")
    await store.get("users", "456")  # Cache miss

    # Access statistics
    stats = store.statistics
    user_stats = stats.get_collection("users")
    print(f"Total gets: {user_stats.get.count}")
    print(f"Cache hits: {user_stats.get.hit}")
    print(f"Cache misses: {user_stats.get.miss}")

asyncio.run(example())
```

Other wrappers that are available include:

- **TTLClampWrapper**: Wraps a store and clamps the TTL to a given range.
- **PassthroughWrapper**: Wraps two stores, using the primary store as a write-through cache for the secondary store. For example, you could use a RedisStore as a distributed primary store and a MemoryStore as the cache store.
- **PrefixCollectionWrapper**: Wraps a store and prefixes all collections with a given prefix.
- **PrefixKeyWrapper**: Wraps a store and prefixes all keys with a given prefix.
- **SingleCollectionWrapper**: Wraps a store and forces all requests into a single collection.

See [DEVELOPING.md](DEVELOPING.md) for more information on how to create your own wrappers.

## Chaining Wrappers, Adapters, and Stores

Imagine you have a service where you want to cache 3 pydantic models in a single collection. You can do this by wrapping the store in a PydanticAdapter and a SingleCollectionWrapper:

```python
import asyncio

from kv_store_adapter.adapters.pydantic import PydanticAdapter
from kv_store_adapter.stores.wrappers.single_collection import SingleCollectionWrapper
from kv_store_adapter.stores.memory import MemoryStore
from pydantic import BaseModel

class User(BaseModel):
    name: str
    email: str

store = MemoryStore()

users_store = PydanticAdapter(SingleCollectionWrapper(store, "users"), User)
products_store = PydanticAdapter(SingleCollectionWrapper(store, "products"), Product)
orders_store = PydanticAdapter(SingleCollectionWrapper(store, "orders"), Order)

async def example():
    new_user: User = User(name="John Doe", email="john.doe@example.com")
    await users_store.put(collection="allowed_users", key="123", value=new_user)

    john_doe: User | None = await users_store.get(collection="allowed_users", key="123")

asyncio.run(example())
```

The SingleCollectionWrapper will result in writes to the `allowed_users` collection being redirected to the `users` collection but the keys will be prefixed with the original collection `allowed_users__` name. So the key `123` will be stored as `allowed_users__123` in the `users` collection.

## Development

See [DEVELOPING.md](DEVELOPING.md) for development setup, testing, and contribution guidelines.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please read [DEVELOPING.md](DEVELOPING.md) for development setup and contribution guidelines.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and changes.
