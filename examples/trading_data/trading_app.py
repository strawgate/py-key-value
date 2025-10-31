"""
Trading data caching application demonstrating advanced py-key-value patterns.

This example shows how to:
- Use PydanticAdapter for type-safe price data storage
- Use LoggingWrapper for observability and debugging
- Use PassthroughCacheWrapper for multi-tier caching (memory + disk)
- Use RetryWrapper for handling transient failures
- Use StatisticsWrapper to track cache hit/miss metrics
"""

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

from key_value.aio.adapters.pydantic import PydanticAdapter
from key_value.aio.stores.disk.store import DiskStore
from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.logging.wrapper import LoggingWrapper
from key_value.aio.wrappers.passthrough_cache.wrapper import PassthroughCacheWrapper
from key_value.aio.wrappers.retry.wrapper import RetryWrapper
from key_value.aio.wrappers.statistics.wrapper import StatisticsWrapper
from pydantic import BaseModel


class PriceData(BaseModel):
    """Trading price data for a symbol."""

    symbol: str
    price: float
    volume: int
    timestamp: datetime


class TradingDataCache:
    """
    Trading data cache with multi-tier caching and logging.

    Uses a memory cache for fast access to recent data, with disk-backed
    persistence for historical data. Logging provides observability into
    cache operations.
    """

    def __init__(self, cache_dir: str = ".trading_cache"):
        # Create cache directory
        Path(cache_dir).mkdir(parents=True, exist_ok=True)

        # Tier 1: Memory cache for fast access
        memory_cache = MemoryStore()

        # Tier 2: Disk cache for historical data
        disk_cache = DiskStore(directory=cache_dir)

        # Wrapper stack (applied inside-out):
        # 1. RetryWrapper - Handle transient failures on disk (3 retries with exponential backoff)
        # 2. LoggingWrapper - Log disk operations for debugging
        # 3. PassthroughCacheWrapper - Two-tier caching (memory → disk)
        # 4. StatisticsWrapper - Track all cache metrics (wraps everything)
        retry_wrapper = RetryWrapper(key_value=disk_cache, max_retries=3, initial_delay=0.1)
        disk_with_logging = LoggingWrapper(key_value=retry_wrapper)

        cache_store = PassthroughCacheWrapper(primary_key_value=disk_with_logging, cache_key_value=memory_cache)

        # Wrap the entire cache stack with statistics to track all operations
        stats = StatisticsWrapper(key_value=cache_store)

        # PydanticAdapter for type-safe price data storage/retrieval
        self.adapter: PydanticAdapter[PriceData] = PydanticAdapter[PriceData](
            key_value=stats,
            pydantic_model=PriceData,
        )

        # Store reference to statistics wrapper for metrics
        self.stats_wrapper = stats
        self.cache_dir = cache_dir

    async def store_price(self, symbol: str, price: float, volume: int, ttl: int | None = None) -> str:
        """
        Store price data for a symbol.

        Args:
            symbol: Trading symbol (e.g., "AAPL", "BTC-USD")
            price: Current price
            volume: Trading volume
            ttl: Time-to-live in seconds (optional)

        Returns:
            Price data ID (timestamp-based)
        """
        price_data = PriceData(symbol=symbol, price=price, volume=volume, timestamp=datetime.now(tz=timezone.utc))

        # Use timestamp as key for chronological ordering
        data_id = price_data.timestamp.isoformat()

        await self.adapter.put(collection=f"symbol:{symbol}", key=data_id, value=price_data, ttl=ttl)

        return data_id

    async def get_price(self, symbol: str, data_id: str) -> PriceData | None:
        """
        Retrieve price data for a symbol.

        Args:
            symbol: Trading symbol
            data_id: Price data identifier (timestamp)

        Returns:
            PriceData if found, None otherwise
        """
        return await self.adapter.get(collection=f"symbol:{symbol}", key=data_id)

    async def get_latest_price(self, symbol: str) -> PriceData | None:
        """
        Get the most recent price data for a symbol.

        Note: This is a simplified implementation. In production, you'd want
        to maintain a separate "latest" key or use a time-series database.

        Args:
            symbol: Trading symbol

        Returns:
            Latest PriceData if available, None otherwise
        """
        # This is a demonstration - in production you'd track the latest key
        # For now, this just returns None to show the API pattern
        return await self.adapter.get(collection=f"symbol:{symbol}", key="latest")

    async def delete_price(self, symbol: str, data_id: str) -> bool:
        """
        Delete price data.

        Args:
            symbol: Trading symbol
            data_id: Price data identifier

        Returns:
            True if deleted, False if not found
        """
        return await self.adapter.delete(collection=f"symbol:{symbol}", key=data_id)

    def get_cache_statistics(self) -> dict[str, int | float]:
        """
        Get cache performance statistics across all symbols.

        Returns:
            Dictionary with aggregated cache metrics (hits, misses, operations)
        """
        if isinstance(self.stats_wrapper, StatisticsWrapper):
            # Aggregate statistics across all collections (symbols)
            total_puts = sum(coll_stats.put.count for coll_stats in self.stats_wrapper.statistics.collections.values())
            total_gets = sum(coll_stats.get.count for coll_stats in self.stats_wrapper.statistics.collections.values())
            total_deletes = sum(coll_stats.delete.count for coll_stats in self.stats_wrapper.statistics.collections.values())
            hits = sum(coll_stats.get.hit for coll_stats in self.stats_wrapper.statistics.collections.values())
            misses = sum(coll_stats.get.miss for coll_stats in self.stats_wrapper.statistics.collections.values())

            return {
                "total_gets": total_gets,
                "cache_hits": hits,
                "cache_misses": misses,
                "hit_rate_percent": round((hits / total_gets * 100) if total_gets > 0 else 0, 2),
                "total_puts": total_puts,
                "total_deletes": total_deletes,
            }
        return {}

    async def cleanup(self):
        """Clean up resources (close stores, etc.)."""
        # In a real application, you'd close any open connections here


async def main():
    """Demonstrate the trading data cache."""
    # Configure logging for the demo
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    cache = TradingDataCache(cache_dir=".demo_trading_cache")

    try:
        # Store price data for different symbols
        print("Storing price data...")
        aapl_id1 = await cache.store_price("AAPL", 150.25, 1000000, ttl=3600)
        aapl_id2 = await cache.store_price("AAPL", 150.50, 1200000, ttl=3600)
        btc_id = await cache.store_price("BTC-USD", 45000.00, 500, ttl=3600)

        print(f"  Stored AAPL prices: {aapl_id1}, {aapl_id2}")
        print(f"  Stored BTC-USD price: {btc_id}")

        # Retrieve price data (first access - will be cache miss on disk)
        print("\nRetrieving price data (first access):")
        aapl_price1 = await cache.get_price("AAPL", aapl_id1)
        if aapl_price1:
            print(f"  AAPL: ${aapl_price1.price} (volume: {aapl_price1.volume})")

        # Retrieve again (should be cache hit from memory)
        print("\nRetrieving same data (second access - from memory cache):")
        aapl_price1_cached = await cache.get_price("AAPL", aapl_id1)
        if aapl_price1_cached:
            print(f"  AAPL: ${aapl_price1_cached.price} (volume: {aapl_price1_cached.volume})")

        # Retrieve different data
        btc_price = await cache.get_price("BTC-USD", btc_id)
        if btc_price:
            print(f"  BTC-USD: ${btc_price.price} (volume: {btc_price.volume})")

        # Show cache statistics
        print("\nCache Statistics:")
        stats = cache.get_cache_statistics()
        for key, value in stats.items():
            print(f"  {key}: {value}")

        # Delete old data
        print(f"\nDeleting old AAPL price: {aapl_id1}")
        deleted = await cache.delete_price("AAPL", aapl_id1)
        print(f"  Deleted: {deleted}")

        # Try to retrieve deleted data
        deleted_price = await cache.get_price("AAPL", aapl_id1)
        print(f"  Retrieved after delete: {deleted_price}")

        # Show final statistics
        print("\nFinal Cache Statistics:")
        stats = cache.get_cache_statistics()
        for key, value in stats.items():
            print(f"  {key}: {value}")

    finally:
        await cache.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
