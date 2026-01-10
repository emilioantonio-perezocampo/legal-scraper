"""
RED Phase Tests: Rate Limiter

Tests for token bucket rate limiter for respectful SCJN scraping.
These tests must FAIL initially (RED phase).
"""
import pytest
import asyncio
import time

# These imports will fail until implementation exists
from src.infrastructure.actors.rate_limiter import (
    RateLimiter,
    NoOpRateLimiter,
)


class TestRateLimiterConfiguration:
    """Tests for RateLimiter configuration."""

    def test_default_requests_per_second(self):
        """Default should be 0.5 requests per second (1 per 2 seconds)."""
        limiter = RateLimiter()
        assert limiter.requests_per_second == 0.5

    def test_custom_requests_per_second(self):
        """Should accept custom rate."""
        limiter = RateLimiter(requests_per_second=1.0)
        assert limiter.requests_per_second == 1.0

    def test_slow_rate(self):
        """Should accept very slow rate."""
        limiter = RateLimiter(requests_per_second=0.1)
        assert limiter.requests_per_second == 0.1


class TestRateLimiterAcquire:
    """Tests for RateLimiter acquire behavior."""

    @pytest.mark.asyncio
    async def test_first_acquire_immediate(self):
        """First acquire should be immediate (has 1 token)."""
        limiter = RateLimiter(requests_per_second=0.5)

        start = time.time()
        await limiter.acquire()
        elapsed = time.time() - start

        # First acquire should be nearly instant
        assert elapsed < 0.1

    @pytest.mark.asyncio
    async def test_second_acquire_waits(self):
        """Second acquire should wait for token replenishment."""
        limiter = RateLimiter(requests_per_second=2.0)  # 1 token per 0.5 seconds

        await limiter.acquire()  # First one is instant

        start = time.time()
        await limiter.acquire()  # Should wait
        elapsed = time.time() - start

        # Should wait roughly 0.5 seconds (1/2.0)
        assert elapsed >= 0.4  # Allow some tolerance
        assert elapsed < 1.0

    @pytest.mark.asyncio
    async def test_acquire_respects_rate(self):
        """Multiple acquires should respect rate limit."""
        limiter = RateLimiter(requests_per_second=5.0)  # 5 per second

        start = time.time()
        for _ in range(3):
            await limiter.acquire()
        elapsed = time.time() - start

        # 3 requests at 5/sec = first instant, then 2 * 0.2 = 0.4 seconds
        assert elapsed >= 0.3  # At least 0.4 seconds for 2 waits

    @pytest.mark.asyncio
    async def test_acquire_is_async(self):
        """Acquire should be non-blocking (async)."""
        limiter = RateLimiter(requests_per_second=1.0)

        # Should be able to call acquire() without blocking
        task = asyncio.create_task(limiter.acquire())
        assert asyncio.iscoroutine(limiter.acquire()) or asyncio.isfuture(task)
        await task


class TestRateLimiterReset:
    """Tests for RateLimiter reset functionality."""

    @pytest.mark.asyncio
    async def test_reset_refills_token(self):
        """Reset should refill token to 1."""
        limiter = RateLimiter(requests_per_second=0.5)

        await limiter.acquire()  # Use the token
        limiter.reset()  # Refill

        start = time.time()
        await limiter.acquire()  # Should be instant now
        elapsed = time.time() - start

        assert elapsed < 0.1

    def test_reset_is_synchronous(self):
        """Reset should be a synchronous operation."""
        limiter = RateLimiter()
        result = limiter.reset()
        assert result is None  # Should return None, not a coroutine


class TestRateLimiterConcurrency:
    """Tests for concurrent access to RateLimiter."""

    @pytest.mark.asyncio
    async def test_concurrent_acquire_serialized(self):
        """Concurrent acquires should be serialized."""
        limiter = RateLimiter(requests_per_second=10.0)  # Fast for testing

        start = time.time()
        # Start multiple acquires concurrently
        await asyncio.gather(
            limiter.acquire(),
            limiter.acquire(),
            limiter.acquire(),
        )
        elapsed = time.time() - start

        # Should take at least 0.2 seconds (2 waits at 10/sec)
        assert elapsed >= 0.15


class TestNoOpRateLimiter:
    """Tests for NoOpRateLimiter (testing helper)."""

    @pytest.mark.asyncio
    async def test_acquire_instant(self):
        """NoOp acquire should be instant."""
        limiter = NoOpRateLimiter()

        start = time.time()
        for _ in range(10):
            await limiter.acquire()
        elapsed = time.time() - start

        # All 10 should be nearly instant
        assert elapsed < 0.1

    def test_reset_does_nothing(self):
        """NoOp reset should do nothing."""
        limiter = NoOpRateLimiter()
        result = limiter.reset()
        assert result is None

    @pytest.mark.asyncio
    async def test_no_op_for_testing(self):
        """NoOp should be usable in tests without delays."""
        limiter = NoOpRateLimiter()

        # Should be able to call many times instantly
        for _ in range(100):
            await limiter.acquire()
