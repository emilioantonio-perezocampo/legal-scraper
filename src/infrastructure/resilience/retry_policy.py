"""
Retry Policy Implementation

Provides configurable retry logic with backoff strategies.
"""
import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Optional, Callable, Any, Set, Type, Tuple


class RetryExhaustedError(Exception):
    """Raised when all retry attempts have been exhausted."""

    def __init__(
        self,
        message: str,
        attempts: int,
        last_exception: Optional[Exception] = None,
    ):
        self.message = message
        self.attempts = attempts
        self.last_exception = last_exception
        super().__init__(message)


@dataclass(frozen=True)
class ExponentialBackoff:
    """
    Exponential backoff configuration.

    Calculates delay as: base_delay * (multiplier ^ attempt) + jitter
    """
    base_delay: float = 1.0
    multiplier: float = 2.0
    max_delay: float = 60.0
    jitter: float = 0.1  # Random factor (0-1)

    def get_delay(self, attempt: int) -> float:
        """
        Calculate delay for given attempt number.

        Args:
            attempt: Attempt number (0-indexed)

        Returns:
            Delay in seconds
        """
        delay = self.base_delay * (self.multiplier ** attempt)
        delay = min(delay, self.max_delay)

        # Add jitter
        if self.jitter > 0:
            jitter_range = delay * self.jitter
            delay += random.uniform(-jitter_range, jitter_range)

        return max(0, delay)


@dataclass
class RetryPolicy:
    """
    Configurable retry policy with backoff support.

    Usage:
        policy = RetryPolicy(max_attempts=3)

        async def risky_call():
            return await http_client.get(url)

        result = await policy.execute(risky_call)

        # Or with decorator
        @policy.retry
        async def protected_call():
            return await risky_operation()
    """
    max_attempts: int = 3
    backoff: ExponentialBackoff = field(default_factory=ExponentialBackoff)
    retryable_exceptions: Set[Type[Exception]] = field(
        default_factory=lambda: {Exception}
    )
    non_retryable_exceptions: Set[Type[Exception]] = field(default_factory=set)
    on_retry: Optional[Callable[[int, Exception, float], Any]] = None

    def should_retry(self, exception: Exception) -> bool:
        """
        Determine if the exception should trigger a retry.

        Args:
            exception: The exception that occurred

        Returns:
            True if should retry, False otherwise
        """
        exc_type = type(exception)

        # Check non-retryable first (higher priority)
        for non_retryable in self.non_retryable_exceptions:
            if isinstance(exception, non_retryable):
                return False

        # Check if exception is retryable
        for retryable in self.retryable_exceptions:
            if isinstance(exception, retryable):
                return True

        return False

    async def execute(
        self,
        func: Callable,
        *args,
        **kwargs,
    ) -> Any:
        """
        Execute function with retry logic.

        Args:
            func: Async function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Result from successful execution

        Raises:
            RetryExhaustedError: If all attempts fail
        """
        last_exception: Optional[Exception] = None

        for attempt in range(self.max_attempts):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_exception = e

                if not self.should_retry(e):
                    raise

                if attempt < self.max_attempts - 1:
                    delay = self.backoff.get_delay(attempt)

                    # Call retry callback if provided
                    if self.on_retry:
                        try:
                            callback_result = self.on_retry(attempt + 1, e, delay)
                            if asyncio.iscoroutine(callback_result):
                                await callback_result
                        except Exception:
                            pass  # Don't let callback errors affect retry

                    await asyncio.sleep(delay)

        raise RetryExhaustedError(
            f"Retry exhausted after {self.max_attempts} attempts",
            attempts=self.max_attempts,
            last_exception=last_exception,
        )

    def retry(self, func: Callable) -> Callable:
        """
        Decorator to add retry logic to a function.

        Args:
            func: Async function to protect

        Returns:
            Function with retry logic
        """
        async def wrapper(*args, **kwargs):
            return await self.execute(func, *args, **kwargs)
        return wrapper


@dataclass(frozen=True)
class RetryResult:
    """
    Result of a retry operation.

    Captures success/failure and metrics.
    """
    success: bool
    attempts: int
    total_time: float
    result: Any = None
    last_exception: Optional[Exception] = None

    @property
    def failed(self) -> bool:
        return not self.success


class RetryWithMetrics:
    """
    Retry policy that captures metrics.

    Provides detailed information about retry attempts.
    """

    def __init__(self, policy: Optional[RetryPolicy] = None):
        self.policy = policy or RetryPolicy()
        self._total_attempts = 0
        self._total_successes = 0
        self._total_failures = 0

    @property
    def total_attempts(self) -> int:
        return self._total_attempts

    @property
    def success_rate(self) -> float:
        if self._total_attempts == 0:
            return 0.0
        return self._total_successes / self._total_attempts

    async def execute(
        self,
        func: Callable,
        *args,
        **kwargs,
    ) -> RetryResult:
        """
        Execute function and return detailed result.

        Args:
            func: Async function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            RetryResult with execution details
        """
        start_time = time.time()
        attempts = 0
        last_exception = None

        for attempt in range(self.policy.max_attempts):
            attempts += 1
            self._total_attempts += 1

            try:
                result = await func(*args, **kwargs)
                self._total_successes += 1
                return RetryResult(
                    success=True,
                    attempts=attempts,
                    total_time=time.time() - start_time,
                    result=result,
                )
            except Exception as e:
                last_exception = e

                if not self.policy.should_retry(e):
                    self._total_failures += 1
                    return RetryResult(
                        success=False,
                        attempts=attempts,
                        total_time=time.time() - start_time,
                        last_exception=e,
                    )

                if attempt < self.policy.max_attempts - 1:
                    delay = self.policy.backoff.get_delay(attempt)
                    await asyncio.sleep(delay)

        self._total_failures += 1
        return RetryResult(
            success=False,
            attempts=attempts,
            total_time=time.time() - start_time,
            last_exception=last_exception,
        )

    def get_stats(self) -> dict:
        """Get retry statistics."""
        return {
            "total_attempts": self._total_attempts,
            "total_successes": self._total_successes,
            "total_failures": self._total_failures,
            "success_rate": self.success_rate,
        }
