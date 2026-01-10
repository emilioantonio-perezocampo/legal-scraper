"""
Tests for Circuit Breaker pattern implementation.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock

from src.infrastructure.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    CircuitOpenError,
)


class TestCircuitBreakerConfig:
    """Tests for CircuitBreakerConfig dataclass."""

    def test_config_defaults(self):
        """Config should have sensible defaults."""
        config = CircuitBreakerConfig()

        assert config.failure_threshold == 5
        assert config.success_threshold == 2
        assert config.timeout_seconds == 30.0
        assert config.excluded_exceptions == set()

    def test_config_custom_values(self):
        """Config should accept custom values."""
        config = CircuitBreakerConfig(
            failure_threshold=3,
            success_threshold=1,
            timeout_seconds=10.0,
        )

        assert config.failure_threshold == 3
        assert config.success_threshold == 1
        assert config.timeout_seconds == 10.0


class TestCircuitBreakerCreation:
    """Tests for CircuitBreaker initialization."""

    def test_breaker_creation(self):
        """Breaker should create with name."""
        breaker = CircuitBreaker(name="test_service")

        assert breaker.name == "test_service"
        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0

    def test_breaker_with_config(self):
        """Breaker should accept config."""
        config = CircuitBreakerConfig(failure_threshold=3)
        breaker = CircuitBreaker(name="test", config=config)

        assert breaker.config.failure_threshold == 3


class TestCircuitBreakerClosedState:
    """Tests for closed state behavior."""

    @pytest.mark.asyncio
    async def test_success_in_closed_state(self):
        """Successful calls should not affect closed state."""
        breaker = CircuitBreaker(name="test")

        async with breaker:
            pass  # Success

        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_failure_increments_count(self):
        """Failures should increment failure count."""
        breaker = CircuitBreaker(name="test")

        try:
            async with breaker:
                raise ValueError("Test error")
        except ValueError:
            pass

        assert breaker.failure_count == 1
        assert breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_success_resets_failure_count(self):
        """Success in closed state should reset failure count."""
        config = CircuitBreakerConfig(failure_threshold=5)
        breaker = CircuitBreaker(name="test", config=config)

        # Create some failures
        for _ in range(3):
            try:
                async with breaker:
                    raise ValueError("Error")
            except ValueError:
                pass

        assert breaker.failure_count == 3

        # Success resets count
        async with breaker:
            pass

        assert breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_threshold_opens_circuit(self):
        """Reaching failure threshold should open circuit."""
        config = CircuitBreakerConfig(failure_threshold=3)
        breaker = CircuitBreaker(name="test", config=config)

        for _ in range(3):
            try:
                async with breaker:
                    raise ValueError("Error")
            except ValueError:
                pass

        assert breaker.state == CircuitState.OPEN


class TestCircuitBreakerOpenState:
    """Tests for open state behavior."""

    @pytest.mark.asyncio
    async def test_open_circuit_blocks_calls(self):
        """Open circuit should block calls."""
        config = CircuitBreakerConfig(
            failure_threshold=1,
            timeout_seconds=60.0,
        )
        breaker = CircuitBreaker(name="test", config=config)

        # Trip the circuit
        try:
            async with breaker:
                raise ValueError("Error")
        except ValueError:
            pass

        assert breaker.state == CircuitState.OPEN

        # Next call should be blocked
        with pytest.raises(CircuitOpenError):
            async with breaker:
                pass

    @pytest.mark.asyncio
    async def test_circuit_open_error_message(self):
        """CircuitOpenError should include breaker name."""
        config = CircuitBreakerConfig(failure_threshold=1)
        breaker = CircuitBreaker(name="my_service", config=config)

        await breaker.force_open()

        with pytest.raises(CircuitOpenError) as exc_info:
            async with breaker:
                pass

        assert "my_service" in str(exc_info.value)


class TestCircuitBreakerHalfOpenState:
    """Tests for half-open state behavior."""

    @pytest.mark.asyncio
    async def test_timeout_moves_to_half_open(self):
        """After timeout, circuit should move to half-open."""
        config = CircuitBreakerConfig(
            failure_threshold=1,
            success_threshold=1,  # Only need 1 success to close
            timeout_seconds=0.1,
        )
        breaker = CircuitBreaker(name="test", config=config)

        # Trip the circuit
        try:
            async with breaker:
                raise ValueError("Error")
        except ValueError:
            pass

        assert breaker.state == CircuitState.OPEN

        # Wait for timeout
        await asyncio.sleep(0.15)

        # Next call should be allowed (half-open) and success closes it
        async with breaker:
            pass

        assert breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_success_in_half_open_closes_circuit(self):
        """Success in half-open should close circuit."""
        config = CircuitBreakerConfig(
            failure_threshold=1,
            success_threshold=2,
            timeout_seconds=0.1,
        )
        breaker = CircuitBreaker(name="test", config=config)

        # Trip the circuit
        try:
            async with breaker:
                raise ValueError("Error")
        except ValueError:
            pass

        await asyncio.sleep(0.15)

        # First success - still half-open
        async with breaker:
            pass

        # Second success - closes circuit
        async with breaker:
            pass

        assert breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_failure_in_half_open_reopens_circuit(self):
        """Failure in half-open should reopen circuit."""
        config = CircuitBreakerConfig(
            failure_threshold=1,
            timeout_seconds=0.1,
        )
        breaker = CircuitBreaker(name="test", config=config)

        # Trip the circuit
        try:
            async with breaker:
                raise ValueError("Error")
        except ValueError:
            pass

        await asyncio.sleep(0.15)

        # Fail in half-open
        try:
            async with breaker:
                raise ValueError("Error again")
        except ValueError:
            pass

        assert breaker.state == CircuitState.OPEN


class TestCircuitBreakerExcludedExceptions:
    """Tests for excluded exception handling."""

    @pytest.mark.asyncio
    async def test_excluded_exception_not_counted(self):
        """Excluded exceptions should not count as failures."""
        config = CircuitBreakerConfig(
            failure_threshold=2,
            excluded_exceptions={KeyError},
        )
        breaker = CircuitBreaker(name="test", config=config)

        # KeyError is excluded
        try:
            async with breaker:
                raise KeyError("Not a failure")
        except KeyError:
            pass

        assert breaker.failure_count == 0

        # ValueError is not excluded
        try:
            async with breaker:
                raise ValueError("This is a failure")
        except ValueError:
            pass

        assert breaker.failure_count == 1


class TestCircuitBreakerDecorator:
    """Tests for the protect decorator."""

    @pytest.mark.asyncio
    async def test_decorator_protects_function(self):
        """Decorator should protect async function."""
        breaker = CircuitBreaker(name="test")

        @breaker.protect
        async def my_func():
            return "success"

        result = await my_func()

        assert result == "success"

    @pytest.mark.asyncio
    async def test_decorator_counts_failures(self):
        """Decorator should count failures."""
        config = CircuitBreakerConfig(failure_threshold=3)
        breaker = CircuitBreaker(name="test", config=config)

        @breaker.protect
        async def failing_func():
            raise ValueError("Error")

        for _ in range(3):
            try:
                await failing_func()
            except ValueError:
                pass

        assert breaker.state == CircuitState.OPEN


class TestCircuitBreakerForceControls:
    """Tests for force_open and force_close."""

    @pytest.mark.asyncio
    async def test_force_open(self):
        """force_open should open the circuit."""
        breaker = CircuitBreaker(name="test")

        assert breaker.state == CircuitState.CLOSED

        await breaker.force_open()

        assert breaker.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_force_close(self):
        """force_close should close the circuit."""
        breaker = CircuitBreaker(name="test")
        await breaker.force_open()

        assert breaker.state == CircuitState.OPEN

        await breaker.force_close()

        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0


class TestCircuitBreakerStats:
    """Tests for statistics."""

    @pytest.mark.asyncio
    async def test_get_stats(self):
        """get_stats should return circuit state."""
        breaker = CircuitBreaker(name="my_breaker")

        stats = breaker.get_stats()

        assert stats["name"] == "my_breaker"
        assert stats["state"] == "closed"
        assert stats["failure_count"] == 0
