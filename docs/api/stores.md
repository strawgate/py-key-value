# Stores

Stores are implementations of the `AsyncKeyValue` protocol that provide actual
storage backends.

## Memory Store

In-memory key-value store, useful for testing and development.

::: key_value.aio.stores.memory.MemoryStore
    options:
      show_source: false
      members:
        - __init__

## Disk Store

Persistent disk-based key-value store using DiskCache.

::: key_value.aio.stores.disk.DiskStore
    options:
      show_source: false
      members:
        - __init__

## Redis Store

Redis-backed key-value store.

::: key_value.aio.stores.redis.RedisStore
    options:
      show_source: false
      members:
        - __init__

## DynamoDB Store

AWS DynamoDB-backed key-value store.

::: key_value.aio.stores.dynamodb.DynamoDBStore
    options:
      show_source: false
      members:
        - __init__

## Elasticsearch Store

Elasticsearch-backed key-value store.

::: key_value.aio.stores.elasticsearch.ElasticsearchStore
    options:
      show_source: false
      members:
        - __init__

## MongoDB Store

MongoDB-backed key-value store.

::: key_value.aio.stores.mongodb.MongoDBStore
    options:
      show_source: false
      members:
        - __init__

## Valkey Store

Valkey-backed key-value store (Redis-compatible).

::: key_value.aio.stores.valkey.ValkeyStore
    options:
      show_source: false
      members:
        - __init__

## Memcached Store

Memcached-backed key-value store.

::: key_value.aio.stores.memcached.MemcachedStore
    options:
      show_source: false
      members:
        - __init__

## Null Store

A no-op store that doesn't persist anything, useful for testing.

::: key_value.aio.stores.null.NullStore
    options:
      show_source: false
      members:
        - __init__
