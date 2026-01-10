from src.infrastructure.actors.base import BaseActor

class SchedulerActor(BaseActor):
    """
    The Orchestrator.
    It holds references to worker actors and triggers them based on time or commands.
    """
    def __init__(self):
        super().__init__()
        self.workers = []

    async def handle_message(self, message):
        # 1. Registration Logic
        if isinstance(message, tuple) and message[0] == "REGISTER_WORKER":
            worker_ref = message[1]
            if worker_ref not in self.workers:
                self.workers.append(worker_ref)
            return

        # 2. Trigger Logic
        if message == "TRIGGER_NOW":
            # Broadcast the start command to all registered workers
            for worker in self.workers:
                await worker.tell("START_SCRAPING")
            return