"""
Tests for Retry Policy implementation.
"""
import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

from src.infrastructure.resilience.retry_policy import (
    RetryPolicy,
    ExponentialBackoff,
    RetryExhaustedError,
    RetryResult,
    RetryWithMetrics,
)


class TestExponentialBackoff:
    """Tests for ExponentialBackoff calculation."""

    def test_backoff_defaults(self):
        """Backoff should have sensible defaults."""
        backoff = ExponentialBackoff()

        assert backoff.base_delay == 1.0
        assert backoff.multiplier == 2.0
        assert backoff.max_delay == 60.0

    def test_backoff_calculation(self):
        """Backoff should calculate exponential delays."""
        backoff = ExponentialBackoff(
            base_delay=1.0,
            multiplier=2.0,
            max_delay=60.0,
            jitter=0,  # Disable jitter for predictable testing
        )

        # First attempt: 1 * 2^0 = 1
        assert backoff.get_delay(0) == 1.0

        # Second attempt: 1 * 2^1 = 2
        assert backoff.get_delay(1) == 2.0

        # Third attempt: 1 * 2^2 = 4
        assert backoff.get_delay(2) == 4.0

    def test_backoff_respects_max_delay(self):
        """Backoff should not exceed max_delay."""
        backoff = ExponentialBackoff(
            base_delay=1.0,
            multiplier=10.0,
            max_delay=5.0,
            jitter=0,
        )

        # Would be 1 * 10^3 = 1000, but capped at 5
        delay = backoff.get_delay(3)
        assert delay == 5.0

    def test_backoff_with_jitter(self):
        """Backoff should add jitter when configured."""
        backoff = ExponentialBackoff(
            base_delay=10.0,
            multiplier=1.0,
            jitter=0.5,
        )

        delays = [backoff.get_delay(0) for _ in range(10)]

        # Delays should vary due to jitter
        assert len(set(delays)) > 1

        # But should stay within range (10 +/- 50%)
        for delay in delays:
            assert 5.0 <= delay <= 15.0

    def test_backoff_is_frozen(self):
        """Backoff should be immutable."""
        backoff = ExponentialBackoff()

        with pytest.raises(Exception):
            backoff.base_delay = 5.0


class TestRetryPolicy:
    """Tests for RetryPolicy configuration."""

    def test_policy_defaults(self):
        """Policy should have sensible defaults."""
        policy = RetryPolicy()

        assert policy.max_attempts == 3
        assert Exception in policy.retryable_exceptions

    def test_policy_custom_attempts(self):
        """Policy should accept custom max_attempts."""
        policy = RetryPolicy(max_attempts=5)

        assert policy.max_attempts == 5

    def test_should_retry_retryable_exception(self):
        """should_retry should return True for retryable exceptions."""
        policy = RetryPolicy(
            retryable_exceptions={ValueError, KeyError},
        )

        assert policy.should_retry(ValueError("test"))
        assert policy.should_retry(KeyError("test"))

    def test_should_retry_non_retryable_exception(self):
        """should_retry should return False for non-retryable exceptions."""
        policy = RetryPolicy(
            retryable_exceptions={ValueError},
            non_retryable_exceptions={RuntimeError},
        )

        assert not policy.should_retry(RuntimeError("test"))

    def test_non_retryable_takes_precedence(self):
        """Non-retryable should take precedence over retryable."""
        policy = RetryPolicy(
            retryable_exceptions={Exception},  # Catch all
            non_retryable_exceptions={ValueError},  # But not ValueError
        )

        assert not policy.should_retry(ValueError("test"))
        assert policy.should_retry(KeyError("test"))


class TestRetryPolicyExecute:
    """Tests for RetryPolicy.execute method."""

    @pytest.mark.asyncio
    async def test_execute_success_first_try(self):
        """Successful call should return immediately."""
        policy = RetryPolicy(max_attempts=3)

        async def success_func():
            return "result"

        result = await policy.execute(success_func)

        assert result == "result"

    @pytest.mark.asyncio
    async def test_execute_retries_on_failure(self):
        """Should retry on retryable exception."""
        policy = RetryPolicy(
            max_attempts=3,
            backoff=ExponentialBackoff(base_delay=0.01, jitter=0),
        )
        attempts = [0]

        async def flaky_func():
            attempts[0] += 1
            if attempts[0] < 3:
                raise ValueError("Temporary error")
            return "success"

        result = await policy.execute(flaky_func)

        assert result == "success"
        assert attempts[0] == 3

    @pytest.mark.asyncio
    async def test_execute_raises_after_exhaustion(self):
        """Should raise RetryExhaustedError after max attempts."""
        policy = RetryPolicy(
            max_attempts=3,
            backoff=ExponentialBackoff(base_delay=0.01, jitter=0),
        )

        async def always_fails():
            raise ValueError("Always fails")

        with pytest.raises(RetryExhaustedError) as exc_info:
            await policy.execute(always_fails)

        assert exc_info.value.attempts == 3
        assert isinstance(exc_info.value.last_exception, ValueError)

    @pytest.mark.asyncio
    async def test_execute_raises_non_retryable_immediately(self):
        """Non-retryable exceptions should not be retried."""
        policy = RetryPolicy(
            max_attempts=3,
            non_retryable_exceptions={RuntimeError},
        )
        attempts = [0]

        async def non_retryable():
            attempts[0] += 1
            raise RuntimeError("Fatal error")

        with pytest.raises(RuntimeError):
            await policy.execute(non_retryable)

        # Should only try once
        assert attempts[0] == 1

    @pytest.mark.asyncio
    async def test_execute_calls_on_retry_callback(self):
        """Should call on_retry callback between attempts."""
        retry_calls = []

        def on_retry(attempt, exception, delay):
            retry_calls.append((attempt, type(exception).__name__, delay))

        policy = RetryPolicy(
            max_attempts=3,
            backoff=ExponentialBackoff(base_delay=0.01, jitter=0),
            on_retry=on_retry,
        )
        attempts = [0]

        async def flaky_func():
            attempts[0] += 1
            if attempts[0] < 3:
                raise ValueError("Error")
            return "done"

        await policy.execute(flaky_func)

        # Should have 2 retry callbacks (before attempts 2 and 3)
        assert len(retry_calls) == 2
        assert retry_calls[0][0] == 1  # First retry
        assert retry_calls[1][0] == 2  # Second retry

    @pytest.mark.asyncio
    async def test_execute_with_async_callback(self):
        """Should work with async on_retry callback."""
        retry_calls = []

        async def async_on_retry(attempt, exception, delay):
            retry_calls.append(attempt)

        policy = RetryPolicy(
            max_attempts=2,
            backoff=ExponentialBackoff(base_delay=0.01, jitter=0),
            on_retry=async_on_retry,
        )

        async def always_fails():
            raise ValueError("Error")

        try:
            await policy.execute(always_fails)
        except RetryExhaustedError:
            pass

        assert len(retry_calls) == 1


class TestRetryPolicyDecorator:
    """Tests for retry decorator."""

    @pytest.mark.asyncio
    async def test_decorator_retries(self):
        """Decorator should add retry logic."""
        policy = RetryPolicy(
            max_attempts=3,
            backoff=ExponentialBackoff(base_delay=0.01, jitter=0),
        )
        attempts = [0]

        @policy.retry
        async def flaky():
            attempts[0] += 1
            if attempts[0] < 2:
                raise ValueError("Error")
            return "success"

        result = await flaky()

        assert result == "success"
        assert attempts[0] == 2


class TestRetryResult:
    """Tests for RetryResult dataclass."""

    def test_success_result(self):
        """Success result should have correct properties."""
        result = RetryResult(
            success=True,
            attempts=1,
            total_time=0.5,
            result="value",
        )

        assert result.success is True
        assert result.failed is False
        assert result.result == "value"

    def test_failure_result(self):
        """Failure result should have correct properties."""
        exc = ValueError("Error")
        result = RetryResult(
            success=False,
            attempts=3,
            total_time=2.5,
            last_exception=exc,
        )

        assert result.success is False
        assert result.failed is True
        assert result.last_exception is exc

    def test_result_is_frozen(self):
        """Result should be immutable."""
        result = RetryResult(success=True, attempts=1, total_time=0.1)

        with pytest.raises(Exception):
            result.success = False


class TestRetryWithMetrics:
    """Tests for RetryWithMetrics class."""

    @pytest.mark.asyncio
    async def test_metrics_tracks_attempts(self):
        """Should track total attempts."""
        metrics = RetryWithMetrics()

        async def success():
            return "ok"

        await metrics.execute(success)

        assert metrics.total_attempts == 1

    @pytest.mark.asyncio
    async def test_metrics_tracks_success_rate(self):
        """Should calculate success rate."""
        policy = RetryPolicy(
            max_attempts=1,
            retryable_exceptions=set(),  # Don't retry
        )
        metrics = RetryWithMetrics(policy=policy)

        async def success():
            return "ok"

        async def failure():
            raise ValueError("Error")

        await metrics.execute(success)
        await metrics.execute(success)

        try:
            await metrics.execute(failure)
        except Exception:
            pass

        # 2 successes out of 3 attempts
        assert metrics.success_rate == pytest.approx(2 / 3, rel=0.01)

    @pytest.mark.asyncio
    async def test_execute_returns_result_object(self):
        """execute should return RetryResult object."""
        metrics = RetryWithMetrics()

        async def success():
            return "value"

        result = await metrics.execute(success)

        assert isinstance(result, RetryResult)
        assert result.success is True
        assert result.result == "value"
        assert result.attempts == 1

    @pytest.mark.asyncio
    async def test_execute_failure_returns_result(self):
        """execute should return result even on failure."""
        policy = RetryPolicy(
            max_attempts=2,
            backoff=ExponentialBackoff(base_delay=0.01, jitter=0),
        )
        metrics = RetryWithMetrics(policy=policy)

        async def always_fails():
            raise ValueError("Error")

        result = await metrics.execute(always_fails)

        assert result.success is False
        assert result.attempts == 2
        assert isinstance(result.last_exception, ValueError)

    @pytest.mark.asyncio
    async def test_get_stats(self):
        """get_stats should return metrics."""
        metrics = RetryWithMetrics()

        stats = metrics.get_stats()

        assert "total_attempts" in stats
        assert "total_successes" in stats
        assert "total_failures" in stats
        assert "success_rate" in stats
