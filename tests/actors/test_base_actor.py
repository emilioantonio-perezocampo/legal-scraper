import pytest
import asyncio
from src.infrastructure.actors.base import BaseActor

# 1. Define a fake actor for testing purposes
class EchoActor(BaseActor):
    def __init__(self):
        super().__init__()
        self.inbox = []  # We'll store received messages here

    async def handle_message(self, message):
        self.inbox.append(message)
        if message == "ping":
            return "pong"

@pytest.mark.asyncio
async def test_actor_receives_message():
    """Test that our BaseActor allows sending and handling messages."""
    actor = EchoActor()
    
    # Start the actor (spin up its background loop)
    await actor.start()

    # Send a fire-and-forget message
    await actor.tell("hello")
    await asyncio.sleep(0.01)  # Give it a tiny moment to process
    
    assert "hello" in actor.inbox
    
    await actor.stop()

@pytest.mark.asyncio
async def test_actor_ask_pattern():
    """Test the Request-Response pattern (Ask)."""
    actor = EchoActor()
    await actor.start()

    # Send a message and wait for the specific reply
    response = await actor.ask("ping")
    
    assert response == "pong"
    await actor.stop()