# Trading Data Cache Example

A trading data caching application demonstrating advanced py-key-value patterns
including logging, multi-tier caching, and retry logic.

## Overview

This example shows how to build an efficient trading data cache using
py-key-value. The implementation features a two-tier caching strategy
(memory + disk) with logging for observability and fast access to recent data.

## Features

- **Type-safe price data storage** using PydanticAdapter
- **Multi-tier caching** with PassthroughCacheWrapper (memory → disk)
- **Operation logging** with LoggingWrapper for debugging and observability
- **Automatic retry** with RetryWrapper for transient failure handling
- **Cache metrics** tracking with StatisticsWrapper
- **Symbol-based isolation** using collection-based storage

## Architecture

The wrapper stack (applied inside-out):

1. **StatisticsWrapper** - Tracks cache hit/miss metrics and operation counts
2. **RetryWrapper** - Handles transient failures with exponential backoff (3
   retries)
3. **LoggingWrapper** - Logs all operations for debugging and monitoring
4. **PassthroughCacheWrapper** - Two-tier caching: fast memory cache with disk
   persistence

Data flow:

- **Write**: Data → Memory cache → Logged → Disk storage
- **Read**: Check memory cache → If miss, load from disk → Log → Cache in memory

## Requirements

- Python 3.10 or newer
- py-key-value-aio
- pydantic

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or using uv
uv pip install -e .
```

## Usage

### Basic Example

```python
import asyncio
from trading_app import TradingDataCache

async def main():
    cache = TradingDataCache(cache_dir=".trading_cache")

    # Store price data
    aapl_id = await cache.store_price("AAPL", 150.25, 1000000, ttl=3600)

    # Retrieve price data (first access - from disk)
    price = await cache.get_price("AAPL", aapl_id)
    print(f"AAPL: ${price.price} (volume: {price.volume})")

    # Retrieve again (second access - from memory cache)
    price = await cache.get_price("AAPL", aapl_id)

    # View cache statistics
    stats = cache.get_cache_statistics()
    print(f"Cache hit rate: {stats['hit_rate_percent']}%")

    await cache.cleanup()

asyncio.run(main())
```

### Running the Demo

```bash
python trading_app.py
```

### Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest test_trading_app.py -v
```

## Key Concepts

### Multi-tier Caching

The PassthroughCacheWrapper creates a two-tier cache:

```python
memory_cache = MemoryStore()  # Fast, volatile
disk_cache = DiskStore()      # Slower, persistent

cache_store = PassthroughCacheWrapper(
    cache=memory_cache,
    key_value=disk_cache
)
```

Benefits:

- **Fast reads**: Recent data served from memory
- **Persistence**: All data backed by disk storage
- **Automatic promotion**: Disk data cached in memory on read

### Compression

CompressionWrapper reduces storage requirements:

```python
compressed_store = CompressionWrapper(key_value=disk_cache)
```

Especially effective for:

- Historical price data with many data points
- JSON-serialized objects with repeated keys
- Text-heavy data structures

### Retry Logic

RetryWrapper handles transient failures:

```python
retry_store = RetryWrapper(
    key_value=base_store,
    max_retries=3,
    base_delay=0.1
)
```

Automatically retries on:

- Network timeouts
- Temporary unavailability
- Rate limiting

### Cache Statistics

Track cache performance with StatisticsWrapper:

```python
stats = cache.get_cache_statistics()
print(f"Hit rate: {stats['hit_rate_percent']}%")
print(f"Total gets: {stats['total_gets']}")
print(f"Cache hits: {stats['cache_hits']}")
print(f"Cache misses: {stats['cache_misses']}")
```

## Performance Considerations

### Memory Cache Size

The memory cache grows unbounded in this example. For production:

```python
# Option 1: Use LRU eviction (if available)
memory_cache = MemoryStore(max_size=1000)

# Option 2: Implement periodic cleanup
async def cleanup_old_entries():
    # Remove entries older than threshold
    pass
```

### Disk Space

Compression helps, but monitor disk usage:

```python
# Add size limits
from key_value.aio.wrappers.limit_size.wrapper import LimitSizeWrapper

limited_disk = LimitSizeWrapper(
    key_value=disk_cache,
    max_size_bytes=1024 * 1024  # 1MB limit per entry
)
```

### Cache Warming

Pre-populate the cache with frequently accessed data:

```python
async def warm_cache(cache: TradingDataCache, symbols: list[str]):
    for symbol in symbols:
        # Load recent data into cache
        await cache.get_latest_price(symbol)
```

## Next Steps

For production use, consider:

1. **Redis Backend**: Replace MemoryStore with RedisStore for distributed
   caching
2. **Encryption**: Add FernetEncryptionWrapper for sensitive price data
3. **TTL Strategy**: Implement smart TTL based on data age and access patterns
4. **Time Series**: Use proper time-series database for historical data
5. **Monitoring**: Export statistics to monitoring systems (Prometheus, etc.)

Example with Redis:

```python
from key_value.aio.stores.redis.store import RedisStore

memory_cache = RedisStore(url="redis://localhost:6379/0")
disk_cache = DiskStore(root_directory="historical_data")

# Build wrapper stack with logging
stats = StatisticsWrapper(key_value=disk_cache)
retry_wrapper = RetryWrapper(key_value=stats, max_retries=3, base_delay=0.1)
disk_with_logging = LoggingWrapper(key_value=retry_wrapper)

cache_store = PassthroughCacheWrapper(
    cache=memory_cache,
    key_value=disk_with_logging
)
```

Example with encryption:

```python
from key_value.aio.wrappers.encryption.wrapper import FernetEncryptionWrapper

# Add encryption to the wrapper stack
stats = StatisticsWrapper(key_value=disk_cache)
encrypted_stats = FernetEncryptionWrapper(
    key_value=stats,
    key=b"your-32-byte-encryption-key-here"
)
retry_wrapper = RetryWrapper(key_value=encrypted_stats, max_retries=3, base_delay=0.1)
disk_with_logging = LoggingWrapper(key_value=retry_wrapper)
```
