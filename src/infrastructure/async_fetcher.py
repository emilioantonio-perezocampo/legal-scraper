"""
Async HTML fetcher with rate limiting and retry logic.
"""
import asyncio
import httpx
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class AsyncFetcher:
    """Async HTTP client for fetching web pages with rate limiting."""

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
    }

    def __init__(
        self,
        rate_limit_delay: float = 2.0,
        timeout: float = 30.0,
        max_retries: int = 3
    ):
        """
        Initialize the fetcher.

        Args:
            rate_limit_delay: Minimum seconds between requests
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
        """
        self.rate_limit_delay = rate_limit_delay
        self.timeout = timeout
        self.max_retries = max_retries
        self._last_request_time = 0.0

    async def fetch(self, url: str) -> Optional[str]:
        """
        Fetch HTML content from URL with rate limiting.

        Args:
            url: The URL to fetch

        Returns:
            HTML content as string, or None on failure
        """
        # Rate limiting
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_request_time
        if elapsed < self.rate_limit_delay:
            wait_time = self.rate_limit_delay - elapsed
            logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
            await asyncio.sleep(wait_time)

        for attempt in range(self.max_retries):
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.get(url, headers=self.DEFAULT_HEADERS)
                    self._last_request_time = asyncio.get_event_loop().time()

                    if response.status_code == 200:
                        logger.info(f"Fetched {url} ({len(response.text)} chars)")
                        return response.text

                    elif response.status_code == 429:
                        # Rate limited by server - wait longer
                        wait_time = (attempt + 1) * 10
                        logger.warning(f"Rate limited (429), waiting {wait_time}s")
                        await asyncio.sleep(wait_time)

                    elif response.status_code >= 500:
                        # Server error - retry with backoff
                        wait_time = 2 ** attempt
                        logger.warning(
                            f"Server error {response.status_code}, "
                            f"retry in {wait_time}s"
                        )
                        await asyncio.sleep(wait_time)

                    else:
                        logger.warning(f"HTTP {response.status_code} for {url}")
                        return None

            except httpx.TimeoutException:
                logger.warning(f"Timeout fetching {url} (attempt {attempt + 1})")
                await asyncio.sleep(2 ** attempt)

            except Exception as e:
                logger.error(f"Fetch error (attempt {attempt + 1}): {e}")
                await asyncio.sleep(2 ** attempt)

        logger.error(f"Failed to fetch {url} after {self.max_retries} attempts")
        return None

    async def fetch_with_status(self, url: str) -> tuple[Optional[str], int]:
        """
        Fetch URL and return both content and status code.

        Returns:
            Tuple of (content, status_code)
        """
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_request_time
        if elapsed < self.rate_limit_delay:
            await asyncio.sleep(self.rate_limit_delay - elapsed)

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url, headers=self.DEFAULT_HEADERS)
                self._last_request_time = asyncio.get_event_loop().time()
                return response.text, response.status_code
        except Exception as e:
            logger.error(f"Fetch error: {e}")
            return None, 0
