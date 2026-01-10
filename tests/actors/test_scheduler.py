import pytest
import asyncio
from src.infrastructure.actors.base import BaseActor
# This will fail - Scheduler doesn't exist yet
from src.infrastructure.actors.scheduler import SchedulerActor 

class MockWorker(BaseActor):
    """A fake worker to see if the Scheduler sends us work."""
    def __init__(self):
        super().__init__()
        self.work_received = False

    async def handle_message(self, message):
        if message == "START_SCRAPING":
            self.work_received = True

@pytest.mark.asyncio
async def test_scheduler_triggers_worker():
    """
    Test that the Scheduler tells the worker to start.
    """
    # 1. Setup
    worker = MockWorker()
    await worker.start()
    
    scheduler = SchedulerActor()
    await scheduler.start()

    # 2. Register the worker
    # We tell the scheduler: "Here is the DOF worker, manage it."
    await scheduler.tell(("REGISTER_WORKER", worker))

    # 3. Trigger the schedule manually (to avoid waiting for real time in tests)
    # In production, this would happen automatically based on the clock.
    await scheduler.tell("TRIGGER_NOW")

    # Allow async propagation
    await asyncio.sleep(0.01)

    # 4. Assert
    assert worker.work_received is True
    
    # Cleanup
    await worker.stop()
    await scheduler.stop()