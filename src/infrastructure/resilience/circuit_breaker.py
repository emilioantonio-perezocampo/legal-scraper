"""
Circuit Breaker Pattern Implementation

Prevents cascading failures by stopping requests to failing services.
"""
import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable, Any, Set, Type


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation, requests allowed
    OPEN = "open"  # Failure threshold exceeded, requests blocked
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitOpenError(Exception):
    """Raised when circuit is open and request is blocked."""

    def __init__(self, message: str = "Circuit breaker is open"):
        self.message = message
        super().__init__(message)


@dataclass
class CircuitBreakerConfig:
    """
    Configuration for circuit breaker behavior.

    Args:
        failure_threshold: Number of failures before opening circuit
        success_threshold: Number of successes in half-open to close circuit
        timeout_seconds: Time to wait before moving from open to half-open
        excluded_exceptions: Exception types that don't count as failures
    """
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 30.0
    excluded_exceptions: Set[Type[Exception]] = field(default_factory=set)


class CircuitBreaker:
    """
    Circuit breaker for protecting against cascading failures.

    Usage:
        breaker = CircuitBreaker(name="scjn_api")

        async def make_request():
            async with breaker:
                return await http_client.get(url)

        # Or with decorator
        @breaker.protect
        async def protected_call():
            return await risky_operation()
    """

    def __init__(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
    ):
        self.name = name
        self.config = config or CircuitBreakerConfig()

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state

    @property
    def failure_count(self) -> int:
        """Get current failure count."""
        return self._failure_count

    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed (normal operation)."""
        return self._state == CircuitState.CLOSED

    @property
    def is_open(self) -> bool:
        """Check if circuit is open (blocking requests)."""
        return self._state == CircuitState.OPEN

    async def __aenter__(self):
        """Context manager entry - check if request allowed."""
        await self._before_call()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - record result."""
        if exc_type is None:
            await self._on_success()
        elif exc_type and not self._is_excluded_exception(exc_type):
            await self._on_failure()
        return False  # Don't suppress exceptions

    def protect(self, func: Callable) -> Callable:
        """
        Decorator to protect a function with the circuit breaker.

        Args:
            func: Async function to protect

        Returns:
            Protected function
        """
        async def wrapper(*args, **kwargs):
            async with self:
                return await func(*args, **kwargs)
        return wrapper

    async def _before_call(self) -> None:
        """Check circuit state before making a call."""
        async with self._lock:
            if self._state == CircuitState.OPEN:
                # Check if timeout has passed
                if self._should_try_reset():
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                else:
                    raise CircuitOpenError(
                        f"Circuit breaker '{self.name}' is open"
                    )

    async def _on_success(self) -> None:
        """Record a successful call."""
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._reset()
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0

    async def _on_failure(self) -> None:
        """Record a failed call."""
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                # Any failure in half-open trips the circuit
                self._state = CircuitState.OPEN
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.config.failure_threshold:
                    self._state = CircuitState.OPEN

    def _should_try_reset(self) -> bool:
        """Check if enough time has passed to try resetting."""
        if self._last_failure_time is None:
            return True
        elapsed = time.time() - self._last_failure_time
        return elapsed >= self.config.timeout_seconds

    def _reset(self) -> None:
        """Reset circuit to closed state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0

    def _is_excluded_exception(self, exc_type: Type[Exception]) -> bool:
        """Check if exception type is excluded from failure counting."""
        return any(
            issubclass(exc_type, excluded)
            for excluded in self.config.excluded_exceptions
        )

    async def force_open(self) -> None:
        """Force the circuit to open (for testing/manual intervention)."""
        async with self._lock:
            self._state = CircuitState.OPEN
            self._last_failure_time = time.time()

    async def force_close(self) -> None:
        """Force the circuit to close (for testing/manual intervention)."""
        async with self._lock:
            self._reset()

    def get_stats(self) -> dict:
        """Get circuit breaker statistics."""
        return {
            "name": self.name,
            "state": self._state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "last_failure_time": self._last_failure_time,
        }
