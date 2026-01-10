"""
CAS Base Actor

Base class for CAS pipeline actors with supervision support.
Implements the Actor Model pattern with message passing.
"""
import asyncio
import logging
import uuid
from typing import Optional, Any
from abc import ABC

from src.infrastructure.actors.cas_messages import (
    EstadoPipelineCAS,
    ObtenerEstadoCAS,
    ErrorCASPipeline,
)


logger = logging.getLogger(__name__)


class CASBaseActor(ABC):
    """
    Base class for CAS pipeline actors.

    Features:
    - Async message processing via mailbox
    - tell() for fire-and-forget messages
    - ask() for request-response patterns
    - Supervisor escalation for errors
    - Lifecycle hooks (on_start, on_stop)
    """

    def __init__(
        self,
        actor_id: Optional[str] = None,
        supervisor: Optional["CASBaseActor"] = None,
    ):
        self._actor_id = actor_id or f"cas-actor-{uuid.uuid4().hex[:8]}"
        self._supervisor = supervisor
        self._estado = EstadoPipelineCAS.INICIANDO
        self._running = False
        self._mailbox: asyncio.Queue = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None
        self._pending_asks: dict = {}

    @property
    def actor_id(self) -> str:
        """Get actor ID."""
        return self._actor_id

    @property
    def supervisor(self) -> Optional["CASBaseActor"]:
        """Get supervisor reference."""
        return self._supervisor

    @property
    def estado(self) -> EstadoPipelineCAS:
        """Get current state."""
        return self._estado

    @property
    def is_running(self) -> bool:
        """Check if actor is running."""
        return self._running

    def _set_estado(self, estado: EstadoPipelineCAS) -> None:
        """Set actor state."""
        self._estado = estado

    async def start(self) -> None:
        """Start the actor."""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._process_mailbox())
        await self.on_start()
        logger.debug(f"Actor {self._actor_id} started")

    async def stop(self) -> None:
        """Stop the actor."""
        if not self._running:
            return

        self._running = False
        await self.on_stop()

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        logger.debug(f"Actor {self._actor_id} stopped")

    async def on_start(self) -> None:
        """Hook called during start. Override in subclasses."""
        pass

    async def on_stop(self) -> None:
        """Hook called during stop. Override in subclasses."""
        pass

    async def tell(self, message: Any) -> None:
        """
        Send message without waiting for response (fire-and-forget).

        Args:
            message: The message to send
        """
        await self._mailbox.put((message, None))

    async def ask(self, message: Any, timeout: float = 30.0) -> Any:
        """
        Send message and wait for response.

        Args:
            message: The message to send
            timeout: Timeout in seconds

        Returns:
            Response from handler

        Raises:
            asyncio.TimeoutError: If timeout exceeded
        """
        response_future: asyncio.Future = asyncio.Future()
        request_id = uuid.uuid4().hex
        self._pending_asks[request_id] = response_future

        await self._mailbox.put((message, request_id))

        try:
            return await asyncio.wait_for(response_future, timeout)
        except asyncio.TimeoutError:
            self._pending_asks.pop(request_id, None)
            raise

    async def receive(self, message: Any) -> Any:
        """
        Process a received message.

        Args:
            message: The message to process

        Returns:
            Optional response
        """
        handler_name = self._get_handler_name(message)
        handler = getattr(self, handler_name, None)

        if handler:
            try:
                return await handler(message)
            except Exception as e:
                logger.error(f"Error handling {type(message).__name__}: {e}")
                error_msg = self._handle_error(e)
                await self.escalate(error_msg)
                return None
        else:
            logger.warning(f"No handler for {type(message).__name__} in {self._actor_id}")
            return await self._handle_unhandled(message)

    async def _handle_unhandled(self, message: Any) -> None:
        """Handle unhandled message types."""
        pass

    def _get_handler_name(self, message: Any) -> str:
        """Get handler method name for message type."""
        # Convert CamelCase to snake_case, handling acronyms like CAS
        import re
        class_name = type(message).__name__
        # Insert underscore between lower->upper or between caps and cap+lower
        # This handles: IniciarDescubrimientoCAS -> iniciar_descubrimiento_cas
        snake_case = re.sub(
            r'(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])',
            '_',
            class_name
        ).lower()
        return f"handle_{snake_case}"

    async def escalate(self, error: ErrorCASPipeline) -> None:
        """
        Escalate error to supervisor.

        Args:
            error: The error to escalate
        """
        if self._supervisor:
            await self._supervisor.tell(error)
        else:
            logger.error(f"No supervisor to escalate: {error.mensaje}")

    def _handle_error(self, exception: Exception) -> ErrorCASPipeline:
        """
        Create error message from exception.

        Args:
            exception: The exception to convert

        Returns:
            ErrorCASPipeline message
        """
        from src.infrastructure.adapters.cas_errors import CASAdapterError

        recuperable = True
        if isinstance(exception, CASAdapterError):
            recuperable = exception.recoverable

        return ErrorCASPipeline(
            mensaje=str(exception),
            tipo_error=type(exception).__name__,
            recuperable=recuperable,
        )

    async def _process_mailbox(self) -> None:
        """Process messages from mailbox."""
        while self._running:
            try:
                message, request_id = await asyncio.wait_for(
                    self._mailbox.get(),
                    timeout=1.0
                )

                result = await self.receive(message)

                if request_id and request_id in self._pending_asks:
                    future = self._pending_asks.pop(request_id)
                    if not future.done():
                        future.set_result(result)

            except asyncio.TimeoutError:
                # No message, continue loop
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in mailbox processing: {e}")

    # Default handlers that can be overridden

    async def handle_obtener_estado_cas(self, msg: ObtenerEstadoCAS) -> EstadoPipelineCAS:
        """Handle state query."""
        return self._estado
