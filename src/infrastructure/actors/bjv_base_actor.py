"""
Base actor class for BJV pipeline.

Provides async message queue and tell/ask patterns
for actor-based message passing.
"""
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass
class ActorMessage:
    """Wrapper for actor messages with metadata."""
    payload: Any
    reply_to: Optional[asyncio.Future] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


class BJVBaseActor(ABC):
    """
    Base actor with async message queue.

    Features:
    - Async mailbox (queue)
    - tell() for fire-and-forget
    - ask() for request-response
    - Lifecycle management (start/stop)
    """

    def __init__(self, nombre: str = "Actor"):
        """
        Initialize the actor.

        Args:
            nombre: Actor name for logging/debugging
        """
        self._nombre = nombre
        self._mailbox: asyncio.Queue = asyncio.Queue()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    @property
    def nombre(self) -> str:
        """Get actor name."""
        return self._nombre

    @property
    def running(self) -> bool:
        """Check if actor is running."""
        return self._running

    async def start(self) -> None:
        """Start the actor's message loop."""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Stop the actor gracefully."""
        self._running = False

        if self._task:
            # Send poison pill to unblock the loop
            await self._mailbox.put(None)

            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except asyncio.TimeoutError:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass

            self._task = None

    async def tell(self, message: Any) -> None:
        """
        Send message without waiting for response.

        Args:
            message: Message payload to send
        """
        await self._mailbox.put(ActorMessage(payload=message))

    async def ask(self, message: Any, timeout: float = 30.0) -> Any:
        """
        Send message and wait for response.

        Args:
            message: Message payload to send
            timeout: Maximum time to wait for response

        Returns:
            Response from handler

        Raises:
            TimeoutError: If no response within timeout
        """
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        await self._mailbox.put(ActorMessage(payload=message, reply_to=future))

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"Actor {self._nombre} no respondió en {timeout}s")

    async def _run_loop(self) -> None:
        """Main message processing loop."""
        while self._running:
            try:
                msg = await self._mailbox.get()

                # Poison pill
                if msg is None:
                    break

                try:
                    result = await self.handle_message(msg.payload)

                    if msg.reply_to and not msg.reply_to.done():
                        msg.reply_to.set_result(result)

                except Exception as e:
                    if msg.reply_to and not msg.reply_to.done():
                        msg.reply_to.set_exception(e)

            except asyncio.CancelledError:
                break
            except Exception:
                # Log error but keep running
                pass

    @abstractmethod
    async def handle_message(self, message: Any) -> Any:
        """
        Process a message. Must be implemented by subclasses.

        Args:
            message: Message payload to process

        Returns:
            Response (used for ask() calls)
        """
        pass
