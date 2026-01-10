"""
Rate limiter for BJV scraping.

Academic resource - be respectful with 0.5 req/sec default.
Uses token bucket algorithm for smooth rate limiting.
"""
import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class RateLimiterConfig:
    """Rate limiter configuration."""
    requests_per_second: float = 0.5  # 1 request per 2 seconds (respectful)
    burst_size: int = 1


class BJVRateLimiter:
    """
    Token bucket rate limiter for BJV requests.

    Default: 0.5 requests/second (respectful of academic resource)
    """

    def __init__(self, config: Optional[RateLimiterConfig] = None):
        """
        Initialize the rate limiter.

        Args:
            config: Rate limiter configuration
        """
        self._config = config or RateLimiterConfig()
        self._tokens = float(self._config.burst_size)
        self._last_update = datetime.utcnow()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a request token is available."""
        async with self._lock:
            now = datetime.utcnow()
            elapsed = (now - self._last_update).total_seconds()

            # Add tokens based on elapsed time
            self._tokens = min(
                float(self._config.burst_size),
                self._tokens + elapsed * self._config.requests_per_second
            )
            self._last_update = now

            if self._tokens < 1:
                # Wait for next token
                wait_time = (1 - self._tokens) / self._config.requests_per_second
                await asyncio.sleep(wait_time)
                self._tokens = 0
            else:
                self._tokens -= 1
