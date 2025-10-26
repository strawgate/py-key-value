"""Tests for the trading data cache example."""

import pytest

from trading_app import PriceData, TradingDataCache


class TestTradingDataCache:
    """Test suite for the TradingDataCache example."""

    @pytest.fixture
    async def cache(self, tmp_path) -> TradingDataCache:
        """Create a TradingDataCache instance for testing."""
        cache = TradingDataCache(cache_dir=str(tmp_path / "test_cache"))
        yield cache
        await cache.cleanup()

    async def test_store_and_retrieve_price(self, cache: TradingDataCache):
        """Test storing and retrieving price data."""
        # Store price data
        data_id = await cache.store_price("AAPL", 150.25, 1000000)

        # Retrieve price data
        price_data = await cache.get_price("AAPL", data_id)

        assert price_data is not None
        assert isinstance(price_data, PriceData)
        assert price_data.symbol == "AAPL"
        assert price_data.price == 150.25
        assert price_data.volume == 1000000
        assert price_data.timestamp is not None

    async def test_store_multiple_prices(self, cache: TradingDataCache):
        """Test storing multiple price points for the same symbol."""
        # Store multiple prices
        id1 = await cache.store_price("BTC-USD", 45000.00, 500)
        id2 = await cache.store_price("BTC-USD", 45100.00, 600)
        id3 = await cache.store_price("BTC-USD", 44900.00, 550)

        # Retrieve all prices
        price1 = await cache.get_price("BTC-USD", id1)
        price2 = await cache.get_price("BTC-USD", id2)
        price3 = await cache.get_price("BTC-USD", id3)

        assert price1 is not None
        assert price1.price == 45000.00
        assert price1.volume == 500

        assert price2 is not None
        assert price2.price == 45100.00
        assert price2.volume == 600

        assert price3 is not None
        assert price3.price == 44900.00
        assert price3.volume == 550

    async def test_delete_price(self, cache: TradingDataCache):
        """Test deleting price data."""
        # Store price data
        data_id = await cache.store_price("ETH-USD", 3000.00, 1000)

        # Verify it exists
        price = await cache.get_price("ETH-USD", data_id)
        assert price is not None

        # Delete the price data
        deleted = await cache.delete_price("ETH-USD", data_id)
        assert deleted is True

        # Verify it's gone
        price = await cache.get_price("ETH-USD", data_id)
        assert price is None

    async def test_delete_nonexistent_price(self, cache: TradingDataCache):
        """Test deleting price data that doesn't exist."""
        deleted = await cache.delete_price("NONEXISTENT", "fake-id")
        assert deleted is False

    async def test_retrieve_nonexistent_price(self, cache: TradingDataCache):
        """Test retrieving price data that doesn't exist."""
        price = await cache.get_price("NONEXISTENT", "fake-id")
        assert price is None

    async def test_symbol_isolation(self, cache: TradingDataCache):
        """Test that price data is isolated between symbols."""
        # Store prices for different symbols
        aapl_id = await cache.store_price("AAPL", 150.00, 1000000)
        msft_id = await cache.store_price("MSFT", 300.00, 500000)

        # Verify prices are in their respective symbol collections
        aapl_in_aapl = await cache.get_price("AAPL", aapl_id)
        aapl_in_msft = await cache.get_price("MSFT", aapl_id)

        assert aapl_in_aapl is not None
        assert aapl_in_aapl.symbol == "AAPL"
        assert aapl_in_msft is None  # Should not be in MSFT collection

        msft_in_msft = await cache.get_price("MSFT", msft_id)
        msft_in_aapl = await cache.get_price("AAPL", msft_id)

        assert msft_in_msft is not None
        assert msft_in_msft.symbol == "MSFT"
        assert msft_in_aapl is None  # Should not be in AAPL collection

    async def test_cache_statistics(self, cache: TradingDataCache):
        """Test that cache statistics are properly tracked."""
        # Perform some operations
        data_id = await cache.store_price("GOOGL", 2500.00, 800000)

        # First get (cache miss on disk, then stored in memory)
        await cache.get_price("GOOGL", data_id)

        # Second get (cache hit from memory)
        await cache.get_price("GOOGL", data_id)

        # Get nonexistent (miss)
        await cache.get_price("GOOGL", "nonexistent")

        # Delete
        await cache.delete_price("GOOGL", data_id)

        # Check statistics
        stats = cache.get_cache_statistics()

        assert "total_gets" in stats
        assert "cache_hits" in stats
        assert "cache_misses" in stats
        assert "total_puts" in stats
        assert "total_deletes" in stats
        assert "hit_rate_percent" in stats

        # We should have at least some operations
        assert stats["total_puts"] >= 1
        assert stats["total_gets"] >= 3
        assert stats["total_deletes"] >= 1

    async def test_price_with_ttl(self, cache: TradingDataCache):
        """Test storing price data with TTL."""
        # Store price with TTL
        data_id = await cache.store_price("TSLA", 800.00, 2000000, ttl=3600)

        # Verify it exists
        price = await cache.get_price("TSLA", data_id)
        assert price is not None
        assert price.symbol == "TSLA"
        assert price.price == 800.00

    async def test_different_symbols(self, cache: TradingDataCache):
        """Test caching different types of trading symbols."""
        # Stock
        stock_id = await cache.store_price("AAPL", 150.00, 1000000)

        # Crypto
        crypto_id = await cache.store_price("BTC-USD", 45000.00, 500)

        # Forex
        forex_id = await cache.store_price("EUR-USD", 1.18, 5000000)

        # Retrieve all
        stock = await cache.get_price("AAPL", stock_id)
        crypto = await cache.get_price("BTC-USD", crypto_id)
        forex = await cache.get_price("EUR-USD", forex_id)

        assert stock is not None
        assert stock.symbol == "AAPL"

        assert crypto is not None
        assert crypto.symbol == "BTC-USD"

        assert forex is not None
        assert forex.symbol == "EUR-USD"
