"""
Web scraper cache demonstrating security and resilience patterns with py-key-value.

This example shows how to:
- Use PydanticAdapter for type-safe scraped data storage
- Apply FernetEncryptionWrapper for encrypted cache storage
- Use LimitSizeWrapper to prevent huge pages from being cached
- Use TTLClampWrapper for controlled cache duration
- Use FallbackWrapper for resilience (memory fallback if disk fails)
"""

import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

from cryptography.fernet import Fernet
from key_value.aio.adapters.pydantic import PydanticAdapter
from key_value.aio.stores.disk.store import DiskStore
from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.encryption.wrapper import FernetEncryptionWrapper
from key_value.aio.wrappers.fallback.wrapper import FallbackWrapper
from key_value.aio.wrappers.limit_size.wrapper import LimitSizeWrapper
from key_value.aio.wrappers.ttl_clamp.wrapper import TTLClampWrapper
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class ScrapedPage(BaseModel):
    """Scraped web page data."""

    url: str
    content: str
    headers: dict[str, str]
    scraped_at: datetime


class WebScraperCache:
    """
    Web scraper cache with encryption, size limits, and fallback resilience.

    Pages are encrypted before storage, with size limits to prevent memory issues.
    Falls back to memory storage if disk operations fail.
    """

    def __init__(self, cache_dir: str = ".scraper_cache", encryption_key: bytes | None = None):
        # Create cache directory
        Path(cache_dir).mkdir(parents=True, exist_ok=True)

        # Generate or use provided encryption key
        if encryption_key is None:
            encryption_key = Fernet.generate_key()
            # Generate a safe fingerprint for identification (never log the actual key!)
            key_fingerprint = hashlib.sha256(encryption_key).hexdigest()[:16]
            logger.warning(f"Generated new encryption key (fingerprint: {key_fingerprint})")
            logger.warning("Store this key securely! Data encrypted with different keys cannot be decrypted.")

        self.encryption_key = encryption_key

        # Primary store: Disk with encryption and size limits
        disk_store = DiskStore(root_directory=cache_dir)

        # Fallback store: Memory (for when disk fails)
        fallback_store = MemoryStore()

        # Wrapper stack (applied inside-out):
        # 1. TTLClampWrapper - Enforce cache duration (min 1 hour, max 7 days)
        # 2. LimitSizeWrapper - Prevent huge pages (max 5MB per page)
        # 3. FernetEncryptionWrapper - Encrypt cached content
        # 4. FallbackWrapper - Fallback to memory if disk fails
        primary_with_wrappers = TTLClampWrapper(
            key_value=LimitSizeWrapper(
                key_value=FernetEncryptionWrapper(
                    key_value=FallbackWrapper(key_value=disk_store, fallback=fallback_store),
                    key=encryption_key,
                ),
                max_size_bytes=5 * 1024 * 1024,  # 5MB limit
            ),
            min_ttl=3600,  # 1 hour minimum
            max_ttl=7 * 24 * 3600,  # 7 days maximum
        )

        # PydanticAdapter for type-safe scraped data storage/retrieval
        self.adapter: PydanticAdapter[ScrapedPage] = PydanticAdapter[ScrapedPage](
            key_value=primary_with_wrappers,
            pydantic_model=ScrapedPage,
        )

        self.cache_dir = cache_dir

    def _url_to_key(self, url: str) -> str:
        """
        Convert URL to a safe cache key.

        Uses SHA-256 hash to create a consistent, filesystem-safe key.

        Args:
            url: The URL to convert

        Returns:
            Hashed key string
        """
        return hashlib.sha256(url.encode()).hexdigest()

    async def cache_page(self, url: str, content: str, headers: dict[str, str] | None = None, ttl: int = 86400) -> bool:
        """
        Cache a scraped web page.

        Args:
            url: Page URL
            content: Page content (HTML, JSON, etc.)
            headers: HTTP headers received
            ttl: Cache duration in seconds (clamped to 1 hour - 7 days)

        Returns:
            True if cached successfully, False if rejected (e.g., too large)
        """
        page = ScrapedPage(url=url, content=content, headers=headers or {}, scraped_at=datetime.now(tz=timezone.utc))

        key = self._url_to_key(url)

        try:
            await self.adapter.put(collection="pages", key=key, value=page, ttl=ttl)
        except Exception:
            logger.exception(f"Failed to cache page {url}")
            return False
        else:
            return True

    async def get_cached_page(self, url: str) -> ScrapedPage | None:
        """
        Retrieve a cached page.

        Args:
            url: Page URL

        Returns:
            ScrapedPage if found and valid, None otherwise
        """
        key = self._url_to_key(url)
        return await self.adapter.get(collection="pages", key=key)

    async def invalidate_page(self, url: str) -> bool:
        """
        Invalidate (delete) a cached page.

        Args:
            url: Page URL

        Returns:
            True if page was deleted, False if not found
        """
        key = self._url_to_key(url)
        return await self.adapter.delete(collection="pages", key=key)

    async def is_cached(self, url: str) -> bool:
        """
        Check if a page is cached.

        Args:
            url: Page URL

        Returns:
            True if page is cached, False otherwise
        """
        page = await self.get_cached_page(url)
        return page is not None

    async def cleanup(self):
        """Clean up resources."""
        # In a real application, you'd close any open connections here


async def simulate_scrape(url: str) -> tuple[str, dict[str, str]]:
    """
    Simulate scraping a web page.

    In a real application, this would use aiohttp or similar to fetch the page.

    Args:
        url: URL to scrape

    Returns:
        Tuple of (content, headers)
    """
    # Simulate network delay
    await asyncio.sleep(0.1)

    # Simulate scraped content
    content = f"<html><body><h1>Content from {url}</h1><p>This is simulated content.</p></body></html>"
    headers = {"content-type": "text/html", "server": "SimulatedServer/1.0"}

    return content, headers


async def main():
    """Demonstrate the web scraper cache."""
    # Generate a key for this demo (in production, load from secure storage)
    encryption_key = Fernet.generate_key()
    # Only show fingerprint, never the actual key
    key_fingerprint = hashlib.sha256(encryption_key).hexdigest()[:16]
    print(f"Using encryption key (fingerprint: {key_fingerprint})\n")

    cache = WebScraperCache(cache_dir=".demo_scraper_cache", encryption_key=encryption_key)

    try:
        urls = [
            "https://example.com/page1",
            "https://example.com/page2",
            "https://example.com/page3",
        ]

        # Scrape and cache pages
        print("Scraping and caching pages...")
        for url in urls:
            # Check if already cached
            if await cache.is_cached(url):
                print(f"  {url}: Already cached (skipping)")
                continue

            # Scrape the page
            content, headers = await simulate_scrape(url)

            # Cache the result
            cached = await cache.cache_page(url, content, headers, ttl=3600)  # 1 hour TTL
            print(f"  {url}: {'Cached' if cached else 'Failed to cache'}")

        # Retrieve cached pages
        print("\nRetrieving cached pages:")
        for url in urls:
            page = await cache.get_cached_page(url)
            if page:
                print(f"  {url}:")
                print(f"    Scraped at: {page.scraped_at}")
                print(f"    Content length: {len(page.content)} bytes")
                print(f"    Headers: {page.headers}")
            else:
                print(f"  {url}: Not found in cache")

        # Demonstrate size limit
        print("\nTesting size limit (5MB):")
        huge_content = "x" * (6 * 1024 * 1024)  # 6MB content
        cached = await cache.cache_page("https://example.com/huge", huge_content, {})
        print(f"  Caching 6MB page: {'Success' if cached else 'Rejected (too large)'}")

        # Invalidate a page
        print(f"\nInvalidating cache for: {urls[0]}")
        invalidated = await cache.invalidate_page(urls[0])
        print(f"  Invalidated: {invalidated}")

        # Try to retrieve invalidated page
        page = await cache.get_cached_page(urls[0])
        print(f"  Retrieved after invalidation: {page}")

        # Check cache status
        print("\nFinal cache status:")
        for url in urls:
            cached = await cache.is_cached(url)
            print(f"  {url}: {'Cached' if cached else 'Not cached'}")

    finally:
        await cache.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
