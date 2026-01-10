"""
Token bucket rate limiter for respectful SCJN scraping.

Implements async rate limiting to avoid overwhelming the SCJN server.
Default rate is 0.5 requests per second (1 request every 2 seconds).
"""
import asyncio
import time
from dataclasses import dataclass, field


@dataclass
class RateLimiter:
    """
    Async token bucket rate limiter.

    Uses a simple token bucket algorithm where:
    - Tokens replenish at `requests_per_second` rate
    - Maximum of 1 token (no bursting)
    - acquire() waits until a token is available

    Default: 0.5 requests/second (1 request per 2 seconds)
    """
    requests_per_second: float = 0.5
    _tokens: float = field(default=1.0, init=False, repr=False)
    _last_update: float = field(default_factory=time.time, init=False, repr=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    async def acquire(self) -> None:
        """
        Wait until a request token is available.

        This method will block (async sleep) if no tokens are available,
        waiting for the token to replenish based on the configured rate.
        """
        async with self._lock:
            now = time.time()
            elapsed = now - self._last_update

            # Replenish tokens based on elapsed time
            self._tokens = min(1.0, self._tokens + elapsed * self.requests_per_second)

            if self._tokens < 1.0:
                # Need to wait for token to replenish
                wait_time = (1.0 - self._tokens) / self.requests_per_second
                await asyncio.sleep(wait_time)
                self._tokens = 0.0
            else:
                # Have a token, consume it
                self._tokens -= 1.0

            self._last_update = time.time()

    def reset(self) -> None:
        """
        Reset to full token.

        Useful for testing or when starting a new batch of requests.
        """
        self._tokens = 1.0
        self._last_update = time.time()


@dataclass
class NoOpRateLimiter:
    """
    Rate limiter that doesn't limit (for testing).

    Provides the same interface as RateLimiter but doesn't
    actually delay requests. Use in tests to avoid waiting.
    """

    async def acquire(self) -> None:
        """Acquire without waiting."""
        pass

    def reset(self) -> None:
        """Reset does nothing."""
        pass
