"""
Tests for BJV base actor.

Following RED-GREEN TDD: These tests define the expected behavior
for the BJV base actor with async mailbox and tell/ask patterns.

Target: ~20 tests
"""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock


class TestActorMessage:
    """Tests for ActorMessage wrapper."""

    def test_creation_with_payload(self):
        """ActorMessage wraps any payload."""
        from src.infrastructure.actors.bjv_base_actor import ActorMessage

        payload = {"key": "value"}
        msg = ActorMessage(payload=payload)

        assert msg.payload == payload

    def test_timestamp_auto_set(self):
        """ActorMessage sets timestamp automatically."""
        from src.infrastructure.actors.bjv_base_actor import ActorMessage

        msg = ActorMessage(payload="test")

        assert msg.timestamp is not None

    def test_reply_to_optional(self):
        """ActorMessage reply_to is optional."""
        from src.infrastructure.actors.bjv_base_actor import ActorMessage

        msg = ActorMessage(payload="test")

        assert msg.reply_to is None

    def test_reply_to_accepts_future(self):
        """ActorMessage accepts Future for reply_to."""
        from src.infrastructure.actors.bjv_base_actor import ActorMessage

        loop = asyncio.new_event_loop()
        future = loop.create_future()
        msg = ActorMessage(payload="test", reply_to=future)

        assert msg.reply_to is future
        loop.close()


class TestBJVBaseActorInit:
    """Tests for BJVBaseActor initialization."""

    def test_init_with_nombre(self):
        """BJVBaseActor initializes with nombre."""
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        class TestActor(BJVBaseActor):
            async def handle_message(self, message):
                pass

        actor = TestActor(nombre="TestActor")

        assert actor.nombre == "TestActor"

    def test_init_default_nombre(self):
        """BJVBaseActor has default nombre."""
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        class TestActor(BJVBaseActor):
            async def handle_message(self, message):
                pass

        actor = TestActor()

        assert actor.nombre == "Actor"

    def test_not_running_initially(self):
        """BJVBaseActor is not running initially."""
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        class TestActor(BJVBaseActor):
            async def handle_message(self, message):
                pass

        actor = TestActor()

        assert actor.running is False


class TestBJVBaseActorLifecycle:
    """Tests for actor start/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_start_creates_task(self):
        """start() creates message loop task."""
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        class TestActor(BJVBaseActor):
            async def handle_message(self, message):
                pass

        actor = TestActor()
        await actor.start()

        assert actor.running is True
        assert actor._task is not None

        await actor.stop()

    @pytest.mark.asyncio
    async def test_stop_cancels_task(self):
        """stop() cancels message loop task."""
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        class TestActor(BJVBaseActor):
            async def handle_message(self, message):
                pass

        actor = TestActor()
        await actor.start()
        await actor.stop()

        assert actor.running is False

    @pytest.mark.asyncio
    async def test_double_start_safe(self):
        """Calling start() twice is safe."""
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        class TestActor(BJVBaseActor):
            async def handle_message(self, message):
                pass

        actor = TestActor()
        await actor.start()
        await actor.start()  # Second start should be no-op

        assert actor.running is True

        await actor.stop()


class TestBJVBaseActorTell:
    """Tests for tell() method."""

    @pytest.mark.asyncio
    async def test_tell_adds_to_mailbox(self):
        """tell() adds message to mailbox."""
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        received = []

        class TestActor(BJVBaseActor):
            async def handle_message(self, message):
                received.append(message)

        actor = TestActor()
        await actor.start()
        await actor.tell("test message")

        # Allow message to be processed
        await asyncio.sleep(0.1)

        assert len(received) == 1
        assert received[0] == "test message"

        await actor.stop()

    @pytest.mark.asyncio
    async def test_multiple_tells_queued(self):
        """Multiple tell() calls are queued."""
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        received = []

        class TestActor(BJVBaseActor):
            async def handle_message(self, message):
                received.append(message)

        actor = TestActor()
        await actor.start()

        await actor.tell("msg1")
        await actor.tell("msg2")
        await actor.tell("msg3")

        await asyncio.sleep(0.1)

        assert len(received) == 3
        assert received == ["msg1", "msg2", "msg3"]

        await actor.stop()


class TestBJVBaseActorAsk:
    """Tests for ask() method."""

    @pytest.mark.asyncio
    async def test_ask_waits_for_response(self):
        """ask() waits for handler response."""
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        class TestActor(BJVBaseActor):
            async def handle_message(self, message):
                return f"response to {message}"

        actor = TestActor()
        await actor.start()

        result = await actor.ask("question")

        assert result == "response to question"

        await actor.stop()

    @pytest.mark.asyncio
    async def test_ask_timeout_raises(self):
        """ask() raises TimeoutError on timeout."""
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        class SlowActor(BJVBaseActor):
            async def handle_message(self, message):
                await asyncio.sleep(10)  # Very slow
                return "done"

        actor = SlowActor()
        await actor.start()

        with pytest.raises(TimeoutError):
            await actor.ask("question", timeout=0.1)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_ask_returns_handler_result(self):
        """ask() returns exact handler result."""
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        class MathActor(BJVBaseActor):
            async def handle_message(self, message):
                return message * 2

        actor = MathActor()
        await actor.start()

        result = await actor.ask(21)

        assert result == 42

        await actor.stop()


class TestConcreteActor:
    """Tests for concrete actor implementations."""

    @pytest.mark.asyncio
    async def test_handle_message_called(self):
        """handle_message is called for each message."""
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        call_count = 0

        class CountingActor(BJVBaseActor):
            async def handle_message(self, message):
                nonlocal call_count
                call_count += 1

        actor = CountingActor()
        await actor.start()

        await actor.tell("a")
        await actor.tell("b")
        await actor.tell("c")

        await asyncio.sleep(0.1)

        assert call_count == 3

        await actor.stop()

    @pytest.mark.asyncio
    async def test_handle_message_result_returned(self):
        """handle_message result is returned via ask."""
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        class EchoActor(BJVBaseActor):
            async def handle_message(self, message):
                return {"echo": message, "processed": True}

        actor = EchoActor()
        await actor.start()

        result = await actor.ask("hello")

        assert result == {"echo": "hello", "processed": True}

        await actor.stop()

    @pytest.mark.asyncio
    async def test_exception_in_handler_propagates_to_ask(self):
        """Exception in handle_message propagates to ask() caller."""
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        class FailingActor(BJVBaseActor):
            async def handle_message(self, message):
                raise ValueError("Handler failed")

        actor = FailingActor()
        await actor.start()

        with pytest.raises(ValueError, match="Handler failed"):
            await actor.ask("trigger")

        await actor.stop()

    @pytest.mark.asyncio
    async def test_poison_pill_stops_loop(self):
        """Poison pill (None) stops the message loop."""
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        class TestActor(BJVBaseActor):
            async def handle_message(self, message):
                pass

        actor = TestActor()
        await actor.start()

        assert actor.running is True

        # Send poison pill directly
        await actor._mailbox.put(None)
        await asyncio.sleep(0.1)

        # Loop should have exited
        assert actor._task.done() or not actor.running
