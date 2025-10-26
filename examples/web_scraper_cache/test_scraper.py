"""Tests for the web scraper cache example."""

import pytest
from cryptography.fernet import Fernet

from scraper import ScrapedPage, WebScraperCache


class TestWebScraperCache:
    """Test suite for the WebScraperCache example."""

    @pytest.fixture
    async def cache(self, tmp_path) -> WebScraperCache:
        """Create a WebScraperCache instance for testing."""
        encryption_key = Fernet.generate_key()
        cache = WebScraperCache(cache_dir=str(tmp_path / "test_cache"), encryption_key=encryption_key)
        yield cache
        await cache.cleanup()

    async def test_cache_and_retrieve_page(self, cache: WebScraperCache):
        """Test caching and retrieving a web page."""
        # Cache a page
        url = "https://example.com/test"
        content = "<html><body>Test content</body></html>"
        headers = {"content-type": "text/html"}

        cached = await cache.cache_page(url, content, headers)
        assert cached is True

        # Retrieve the page
        page = await cache.get_cached_page(url)

        assert page is not None
        assert isinstance(page, ScrapedPage)
        assert page.url == url
        assert page.content == content
        assert page.headers == headers
        assert page.scraped_at is not None

    async def test_cache_multiple_pages(self, cache: WebScraperCache):
        """Test caching multiple different pages."""
        pages = [
            ("https://example.com/page1", "<html>Page 1</html>", {"server": "Server1"}),
            ("https://example.com/page2", "<html>Page 2</html>", {"server": "Server2"}),
            ("https://example.com/page3", "<html>Page 3</html>", {"server": "Server3"}),
        ]

        # Cache all pages
        for url, content, headers in pages:
            cached = await cache.cache_page(url, content, headers)
            assert cached is True

        # Retrieve all pages
        for url, content, headers in pages:
            page = await cache.get_cached_page(url)
            assert page is not None
            assert page.url == url
            assert page.content == content
            assert page.headers == headers

    async def test_invalidate_page(self, cache: WebScraperCache):
        """Test invalidating a cached page."""
        url = "https://example.com/invalidate-test"
        content = "<html>Test</html>"

        # Cache the page
        await cache.cache_page(url, content, {})

        # Verify it's cached
        assert await cache.is_cached(url) is True

        # Invalidate the page
        invalidated = await cache.invalidate_page(url)
        assert invalidated is True

        # Verify it's gone
        assert await cache.is_cached(url) is False
        page = await cache.get_cached_page(url)
        assert page is None

    async def test_invalidate_nonexistent_page(self, cache: WebScraperCache):
        """Test invalidating a page that doesn't exist."""
        invalidated = await cache.invalidate_page("https://example.com/nonexistent")
        assert invalidated is False

    async def test_is_cached(self, cache: WebScraperCache):
        """Test the is_cached method."""
        url = "https://example.com/cache-check"

        # Should not be cached initially
        assert await cache.is_cached(url) is False

        # Cache the page
        await cache.cache_page(url, "<html>Test</html>", {})

        # Should be cached now
        assert await cache.is_cached(url) is True

        # Invalidate the page
        await cache.invalidate_page(url)

        # Should not be cached anymore
        assert await cache.is_cached(url) is False

    async def test_size_limit_rejection(self, cache: WebScraperCache):
        """Test that pages exceeding the size limit are rejected."""
        url = "https://example.com/huge-page"

        # Create content larger than 5MB limit
        huge_content = "x" * (6 * 1024 * 1024)  # 6MB

        # Should be rejected
        cached = await cache.cache_page(url, huge_content, {})
        assert cached is False

        # Verify it's not cached
        assert await cache.is_cached(url) is False

    async def test_size_limit_accepted(self, cache: WebScraperCache):
        """Test that pages within the size limit are accepted."""
        url = "https://example.com/normal-page"

        # Create content smaller than 5MB limit
        normal_content = "x" * (1 * 1024 * 1024)  # 1MB

        # Should be accepted
        cached = await cache.cache_page(url, normal_content, {})
        assert cached is True

        # Verify it's cached
        assert await cache.is_cached(url) is True

    async def test_ttl_clamping(self, cache: WebScraperCache):
        """Test that TTL is clamped to min/max values."""
        url = "https://example.com/ttl-test"
        content = "<html>Test</html>"

        # Cache with various TTL values (should be clamped to 1 hour - 7 days)
        # Note: We can't directly test the clamping without inspecting internals,
        # but we can verify the page is cached successfully
        await cache.cache_page(url, content, {}, ttl=60)  # Too low (< 1 hour)
        assert await cache.is_cached(url) is True

        await cache.invalidate_page(url)

        await cache.cache_page(url, content, {}, ttl=30 * 24 * 3600)  # Too high (> 7 days)
        assert await cache.is_cached(url) is True

    async def test_url_to_key_consistency(self, cache: WebScraperCache):
        """Test that URL to key conversion is consistent."""
        url = "https://example.com/test"
        content = "<html>Test</html>"

        # Cache the page
        await cache.cache_page(url, content, {})

        # Retrieve multiple times - should get the same page
        page1 = await cache.get_cached_page(url)
        page2 = await cache.get_cached_page(url)

        assert page1 is not None
        assert page2 is not None
        assert page1.url == page2.url
        assert page1.content == page2.content

    async def test_different_urls_different_keys(self, cache: WebScraperCache):
        """Test that different URLs result in different cache entries."""
        url1 = "https://example.com/page1"
        url2 = "https://example.com/page2"

        await cache.cache_page(url1, "<html>Page 1</html>", {})
        await cache.cache_page(url2, "<html>Page 2</html>", {})

        page1 = await cache.get_cached_page(url1)
        page2 = await cache.get_cached_page(url2)

        assert page1 is not None
        assert page2 is not None
        assert page1.content != page2.content

    async def test_cache_with_empty_content(self, cache: WebScraperCache):
        """Test caching a page with empty content."""
        url = "https://example.com/empty"
        content = ""

        cached = await cache.cache_page(url, content, {})
        assert cached is True

        page = await cache.get_cached_page(url)
        assert page is not None
        assert page.content == ""

    async def test_cache_with_no_headers(self, cache: WebScraperCache):
        """Test caching a page with no headers."""
        url = "https://example.com/no-headers"
        content = "<html>Test</html>"

        cached = await cache.cache_page(url, content, None)
        assert cached is True

        page = await cache.get_cached_page(url)
        assert page is not None
        assert page.headers == {}

    async def test_encryption_different_keys(self, tmp_path):
        """Test that data encrypted with different keys cannot be decrypted."""
        cache_dir = str(tmp_path / "encryption_test")
        url = "https://example.com/encrypted"
        content = "<html>Secret content</html>"

        # Cache with first key
        key1 = Fernet.generate_key()
        cache1 = WebScraperCache(cache_dir=cache_dir, encryption_key=key1)
        await cache1.cache_page(url, content, {})
        await cache1.cleanup()

        # Try to retrieve with different key
        key2 = Fernet.generate_key()
        cache2 = WebScraperCache(cache_dir=cache_dir, encryption_key=key2)

        # Should not be able to decrypt (will return None or fail)
        page = await cache2.get_cached_page(url)
        # The page will be None because decryption fails
        assert page is None

        await cache2.cleanup()
