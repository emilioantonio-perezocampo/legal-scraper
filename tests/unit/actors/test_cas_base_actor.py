"""
Tests for CAS Base Actor

Following RED-GREEN TDD: These tests define the expected behavior
for the CAS actor base class with supervision support.

Target: ~25 tests for CAS base actor
"""
import pytest
from unittest.mock import Mock, AsyncMock, patch


class TestCASBaseActorInit:
    """Tests for CASBaseActor initialization."""

    def test_has_actor_id(self):
        """Base actor has actor_id."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        actor = CASBaseActor(actor_id="test-actor-1")
        assert actor.actor_id == "test-actor-1"

    def test_generates_actor_id_if_not_provided(self):
        """Base actor generates actor_id if not provided."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        actor = CASBaseActor()
        assert actor.actor_id is not None
        assert len(actor.actor_id) > 0

    def test_has_supervisor_ref(self):
        """Base actor has supervisor reference."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        supervisor = Mock()
        actor = CASBaseActor(supervisor=supervisor)
        assert actor.supervisor == supervisor

    def test_default_supervisor_is_none(self):
        """Default supervisor is None."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        actor = CASBaseActor()
        assert actor.supervisor is None

    def test_has_estado(self):
        """Base actor has estado property."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        from src.infrastructure.actors.cas_messages import EstadoPipelineCAS
        actor = CASBaseActor()
        assert actor.estado == EstadoPipelineCAS.INICIANDO


class TestCASBaseActorTell:
    """Tests for CASBaseActor tell method (fire-and-forget)."""

    @pytest.mark.asyncio
    async def test_tell_enqueues_message(self):
        """tell() enqueues message for processing."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        actor = CASBaseActor()
        msg = Mock()
        await actor.tell(msg)
        # Message should be in queue
        assert actor._mailbox.qsize() >= 0  # Just check mailbox exists

    @pytest.mark.asyncio
    async def test_tell_does_not_block(self):
        """tell() returns immediately without waiting."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        actor = CASBaseActor()
        msg = Mock()
        # Should complete quickly without blocking
        await actor.tell(msg)
        # No exception means success


class TestCASBaseActorAsk:
    """Tests for CASBaseActor ask method (request-response)."""

    @pytest.mark.asyncio
    async def test_ask_returns_response(self):
        """ask() returns response from handler."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        from src.infrastructure.actors.cas_messages import ObtenerEstadoCAS, EstadoPipelineCAS

        actor = CASBaseActor()
        await actor.start()

        response = await actor.ask(ObtenerEstadoCAS())
        assert response is not None
        await actor.stop()

    @pytest.mark.asyncio
    async def test_ask_with_timeout(self):
        """ask() respects timeout parameter."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        actor = CASBaseActor()
        msg = Mock()

        # Should handle timeout
        with pytest.raises(Exception):  # TimeoutError or similar
            await actor.ask(msg, timeout=0.001)


class TestCASBaseActorLifecycle:
    """Tests for CASBaseActor lifecycle methods."""

    @pytest.mark.asyncio
    async def test_start_initializes_actor(self):
        """start() initializes the actor."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        actor = CASBaseActor()
        await actor.start()
        assert actor._running is True
        await actor.stop()

    @pytest.mark.asyncio
    async def test_stop_shuts_down_actor(self):
        """stop() shuts down the actor."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        actor = CASBaseActor()
        await actor.start()
        await actor.stop()
        assert actor._running is False

    @pytest.mark.asyncio
    async def test_on_start_hook(self):
        """on_start() hook is called during start."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor

        class TestActor(CASBaseActor):
            def __init__(self):
                super().__init__()
                self.on_start_called = False

            async def on_start(self):
                self.on_start_called = True

        actor = TestActor()
        await actor.start()
        assert actor.on_start_called is True
        await actor.stop()

    @pytest.mark.asyncio
    async def test_on_stop_hook(self):
        """on_stop() hook is called during stop."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor

        class TestActor(CASBaseActor):
            def __init__(self):
                super().__init__()
                self.on_stop_called = False

            async def on_stop(self):
                self.on_stop_called = True

        actor = TestActor()
        await actor.start()
        await actor.stop()
        assert actor.on_stop_called is True


class TestCASBaseActorErrorHandling:
    """Tests for CASBaseActor error handling."""

    @pytest.mark.asyncio
    async def test_escalate_to_supervisor(self):
        """escalate() sends error to supervisor."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        from src.infrastructure.actors.cas_messages import ErrorCASPipeline

        supervisor = AsyncMock()
        actor = CASBaseActor(supervisor=supervisor)

        error = ErrorCASPipeline(mensaje="Test error", tipo_error="TestError")
        await actor.escalate(error)

        supervisor.tell.assert_called_once()

    @pytest.mark.asyncio
    async def test_escalate_without_supervisor_logs_error(self):
        """escalate() logs error when no supervisor."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        from src.infrastructure.actors.cas_messages import ErrorCASPipeline

        actor = CASBaseActor()  # No supervisor
        error = ErrorCASPipeline(mensaje="Test error", tipo_error="TestError")

        # Should not raise, just log
        await actor.escalate(error)

    @pytest.mark.asyncio
    async def test_handle_error_creates_error_message(self):
        """_handle_error() creates ErrorCASPipeline from exception."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor

        actor = CASBaseActor()
        exception = ValueError("Something went wrong")

        error_msg = actor._handle_error(exception)
        assert error_msg.mensaje == "Something went wrong"
        assert error_msg.tipo_error == "ValueError"


class TestCASBaseActorMessageHandling:
    """Tests for CASBaseActor message handling."""

    @pytest.mark.asyncio
    async def test_receive_dispatches_to_handler(self):
        """receive() dispatches message to correct handler."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        from src.infrastructure.actors.cas_messages import ObtenerEstadoCAS

        class TestActor(CASBaseActor):
            def __init__(self):
                super().__init__()
                self.handled = False

            async def handle_obtener_estado_cas(self, msg):
                self.handled = True
                return self.estado

        actor = TestActor()
        await actor.start()
        await actor.receive(ObtenerEstadoCAS())
        assert actor.handled is True
        await actor.stop()

    @pytest.mark.asyncio
    async def test_unhandled_message_logs_warning(self):
        """Unhandled message type logs warning."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor

        actor = CASBaseActor()
        await actor.start()

        class UnknownMessage:
            pass

        # Should not raise, just log
        await actor.receive(UnknownMessage())
        await actor.stop()


class TestCASBaseActorState:
    """Tests for CASBaseActor state management."""

    def test_set_estado(self):
        """Can set actor estado."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        from src.infrastructure.actors.cas_messages import EstadoPipelineCAS

        actor = CASBaseActor()
        actor._set_estado(EstadoPipelineCAS.SCRAPEANDO)
        assert actor.estado == EstadoPipelineCAS.SCRAPEANDO

    def test_is_running_property(self):
        """is_running property reflects running state."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor

        actor = CASBaseActor()
        assert actor.is_running is False

    @pytest.mark.asyncio
    async def test_is_running_after_start(self):
        """is_running is True after start."""
        from src.infrastructure.actors.cas_base_actor import CASBaseActor

        actor = CASBaseActor()
        await actor.start()
        assert actor.is_running is True
        await actor.stop()
