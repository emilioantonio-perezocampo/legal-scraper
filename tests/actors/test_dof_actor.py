import pytest
from datetime import date
from src.domain.entities import FederalLaw
# This import will fail because the file doesn't exist yet
from src.infrastructure.actors.dof_actor import DofScraperActor
from src.infrastructure.actors.base import BaseActor

class MockPersistence(BaseActor):
    """Fake actor to check if it receives the law."""
    def __init__(self):
        super().__init__()
        self.received_law = None

    async def handle_message(self, message):
        if isinstance(message, tuple) and message[0] == "SAVE_LAW":
            self.received_law = message[1]

@pytest.mark.asyncio
async def test_dof_actor_pipes_to_persistence():
    """
    Test that the scraper automatically forwards the result to the 
    persistence actor if one is configured.
    """
    # 1. Setup the pipe
    persistence = MockPersistence()
    await persistence.start()

    # Pass the persistence actor into the scraper
    scraper = DofScraperActor(output_actor=persistence)
    await scraper.start()

    # 2. Trigger the scrape (Fire and Forget)
    await scraper.tell("START_SCRAPING")

    # Give it time to process and forward
    import asyncio
    await asyncio.sleep(0.01)

    # 3. Assert: Did the persistence actor get the law?
    assert persistence.received_law is not None
    assert persistence.received_law.title == "Ley Federal del Trabajo"

    await scraper.stop()
    await persistence.stop()
    
@pytest.mark.asyncio
async def test_dof_actor_returns_federal_law():
    """
    Test that the DOF actor acts as an Anti-Corruption Layer (ACL).
    It should take a raw request and return a clean Domain Entity.
    """
    actor = DofScraperActor()
    await actor.start()

    # We ask the actor to "scrape" a specific law ID
    # In a real scenario, this would trigger an HTTP request.
    # For this unit test, the actor will simulate the scraping.
    result = await actor.ask("scrape_ley_federal_trabajo")

    await actor.stop()

    # Assertions: Did we get a valid Domain Object back?
    assert isinstance(result, FederalLaw)
    assert result.title == "Ley Federal del Trabajo"
    assert result.jurisdiction == "Federal"
    assert len(result.articles) > 0
    
@pytest.mark.asyncio
async def test_dof_actor_handles_generic_start_command():
    """
    Test that the actor responds to the standard START_SCRAPING command
    sent by the Scheduler.
    """
    actor = DofScraperActor()
    await actor.start()

    # The Scheduler sends this command genericially
    result = await actor.ask("START_SCRAPING")

    await actor.stop()

    # It should return the scraped data
    assert isinstance(result, FederalLaw)