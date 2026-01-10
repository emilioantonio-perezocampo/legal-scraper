import asyncio
from abc import ABC, abstractmethod

class BaseActor(ABC):
    """
    Abstract Base Actor.
    Runs an internal infinite loop to process messages from a Queue.
    """
    def __init__(self):
        self._queue = asyncio.Queue()
        self._running = False
        self._task = None

    async def start(self):
        """Starts the actor's processing loop in the background."""
        self._running = True
        self._task = asyncio.create_task(self._process_mailbox())

    async def stop(self):
        """Stops the actor cleanly."""
        self._running = False
        await self._queue.put(None)  # Poison pill to break the loop
        if self._task:
            await self._task

    async def tell(self, message):
        """Fire and forget: Send a message without waiting."""
        await self._queue.put(message)

    async def ask(self, message):
        """Request-Response: Send a message and wait for a reply."""
        future = asyncio.get_running_loop().create_future()
        # Wrap the message so we know to send the result back to this future
        await self._queue.put((message, future))
        return await future

    async def _process_mailbox(self):
        """Internal loop that pulls messages off the queue."""
        while self._running:
            item = await self._queue.get()
            
            if item is None:  # Poison pill check
                break

            # Check if it's an "Ask" (Tuple) or a "Tell" (Raw Message)
            if isinstance(item, tuple) and len(item) == 2 and isinstance(item[1], asyncio.Future):
                msg, future = item
                try:
                    result = await self.handle_message(msg)
                    if not future.done():
                        future.set_result(result)
                except Exception as e:
                    if not future.done():
                        future.set_exception(e)
            else:
                # It's a standard Tell message
                await self.handle_message(item)

    @abstractmethod
    async def handle_message(self, message):
        """Subclasses must implement this to define behavior."""
        pass