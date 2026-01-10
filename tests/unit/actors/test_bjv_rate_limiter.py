"""
Tests for BJV rate limiter.

Following RED-GREEN TDD: These tests define the expected behavior
for the token bucket rate limiter for BJV scraping.

Target: ~15 tests
"""
import pytest
import asyncio
import time


class TestRateLimiterConfig:
    """Tests for RateLimiterConfig dataclass."""

    def test_default_values(self):
        """RateLimiterConfig has respectful defaults for academic resource."""
        from src.infrastructure.actors.bjv_rate_limiter import RateLimiterConfig

        config = RateLimiterConfig()

        assert config.requests_per_second == 0.5  # 1 request per 2 seconds
        assert config.burst_size == 1

    def test_custom_values(self):
        """RateLimiterConfig accepts custom values."""
        from src.infrastructure.actors.bjv_rate_limiter import RateLimiterConfig

        config = RateLimiterConfig(
            requests_per_second=1.0,
            burst_size=5,
        )

        assert config.requests_per_second == 1.0
        assert config.burst_size == 5

    def test_frozen(self):
        """RateLimiterConfig is immutable."""
        from src.infrastructure.actors.bjv_rate_limiter import RateLimiterConfig

        config = RateLimiterConfig()

        with pytest.raises(Exception):  # FrozenInstanceError
            config.requests_per_second = 10.0


class TestBJVRateLimiter:
    """Tests for BJVRateLimiter class."""

    def test_creation_default_config(self):
        """BJVRateLimiter creates with default config."""
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter

        limiter = BJVRateLimiter()

        assert limiter is not None
        assert limiter._config.requests_per_second == 0.5

    def test_creation_custom_config(self):
        """BJVRateLimiter creates with custom config."""
        from src.infrastructure.actors.bjv_rate_limiter import (
            BJVRateLimiter,
            RateLimiterConfig,
        )

        config = RateLimiterConfig(requests_per_second=2.0)
        limiter = BJVRateLimiter(config=config)

        assert limiter._config.requests_per_second == 2.0

    @pytest.mark.asyncio
    async def test_first_request_immediate(self):
        """First request is immediate (uses initial token)."""
        from src.infrastructure.actors.bjv_rate_limiter import (
            BJVRateLimiter,
            RateLimiterConfig,
        )

        config = RateLimiterConfig(requests_per_second=1.0, burst_size=1)
        limiter = BJVRateLimiter(config=config)

        start = time.time()
        await limiter.acquire()
        elapsed = time.time() - start

        assert elapsed < 0.1  # Should be nearly instant

    @pytest.mark.asyncio
    async def test_second_request_waits(self):
        """Second request waits for token refill."""
        from src.infrastructure.actors.bjv_rate_limiter import (
            BJVRateLimiter,
            RateLimiterConfig,
        )

        # 2 requests per second = 500ms between requests
        config = RateLimiterConfig(requests_per_second=2.0, burst_size=1)
        limiter = BJVRateLimiter(config=config)

        await limiter.acquire()  # Use initial token

        start = time.time()
        await limiter.acquire()  # Should wait
        elapsed = time.time() - start

        # Should wait approximately 0.5 seconds
        assert elapsed >= 0.4

    @pytest.mark.asyncio
    async def test_burst_allows_multiple(self):
        """Burst size allows multiple immediate requests."""
        from src.infrastructure.actors.bjv_rate_limiter import (
            BJVRateLimiter,
            RateLimiterConfig,
        )

        config = RateLimiterConfig(requests_per_second=1.0, burst_size=3)
        limiter = BJVRateLimiter(config=config)

        start = time.time()
        await limiter.acquire()
        await limiter.acquire()
        await limiter.acquire()
        elapsed = time.time() - start

        # All 3 should be quick (using burst)
        assert elapsed < 0.3

    @pytest.mark.asyncio
    async def test_token_refill_over_time(self):
        """Tokens refill over time."""
        from src.infrastructure.actors.bjv_rate_limiter import (
            BJVRateLimiter,
            RateLimiterConfig,
        )

        # 2 requests per second
        config = RateLimiterConfig(requests_per_second=2.0, burst_size=1)
        limiter = BJVRateLimiter(config=config)

        await limiter.acquire()  # Use token

        # Wait for refill
        await asyncio.sleep(0.6)

        start = time.time()
        await limiter.acquire()  # Should be quick
        elapsed = time.time() - start

        assert elapsed < 0.2  # Token should have refilled

    @pytest.mark.asyncio
    async def test_concurrent_acquire_serialized(self):
        """Concurrent acquire calls are serialized."""
        from src.infrastructure.actors.bjv_rate_limiter import (
            BJVRateLimiter,
            RateLimiterConfig,
        )

        config = RateLimiterConfig(requests_per_second=5.0, burst_size=1)
        limiter = BJVRateLimiter(config=config)

        results = []

        async def acquire_and_record(index):
            await limiter.acquire()
            results.append(index)

        # Launch 3 concurrent acquires
        await asyncio.gather(
            acquire_and_record(1),
            acquire_and_record(2),
            acquire_and_record(3),
        )

        # All should complete
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_respects_slow_rate(self):
        """Respects very slow rate (academic-friendly)."""
        from src.infrastructure.actors.bjv_rate_limiter import (
            BJVRateLimiter,
            RateLimiterConfig,
        )

        # 0.5 requests per second = 2 seconds between requests
        config = RateLimiterConfig(requests_per_second=0.5, burst_size=1)
        limiter = BJVRateLimiter(config=config)

        await limiter.acquire()  # Use initial token

        start = time.time()
        await limiter.acquire()
        elapsed = time.time() - start

        # Should wait approximately 2 seconds
        assert elapsed >= 1.8

    @pytest.mark.asyncio
    async def test_tokens_cap_at_burst_size(self):
        """Tokens don't exceed burst size."""
        from src.infrastructure.actors.bjv_rate_limiter import (
            BJVRateLimiter,
            RateLimiterConfig,
        )

        config = RateLimiterConfig(requests_per_second=10.0, burst_size=2)
        limiter = BJVRateLimiter(config=config)

        # Wait for potential over-accumulation
        await asyncio.sleep(0.5)

        start = time.time()
        await limiter.acquire()
        await limiter.acquire()
        # Third should wait
        await limiter.acquire()
        elapsed = time.time() - start

        # Third request should have caused some waiting
        assert elapsed >= 0.05
