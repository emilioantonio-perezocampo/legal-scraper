"""
GUI Bridge Actor

Bridges the GUI to the existing scraper actor system.
Translates GUI commands into scraper actor messages and
forwards scraper events back to the GUI.
"""
from typing import Optional, Any, Callable
from datetime import date
import asyncio

from src.infrastructure.actors.base import BaseActor
from src.gui.domain.entities import (
    ScraperConfiguration,
    JobProgress,
    LogLevel,
)
from src.gui.domain.value_objects import ScraperMode
from src.gui.domain.events import (
    JobProgressEvent,
    JobCompletedEvent,
    JobFailedEvent,
    LogAddedEvent,
)


class GuiBridgeActor(BaseActor):
    """
    Actor that bridges the GUI with the existing scraper actor system.

    Responsibilities:
    - Connect to existing scraper actors (Scheduler, Discovery, Persistence)
    - Translate GUI commands into scraper actor messages
    - Receive events from scrapers and forward to GUI
    - Handle connection management

    Messages:
    - ("START_SCRAPE", config) -> Starts scraping based on configuration
    - "STOP_SCRAPE" -> Stops current scraping operation
    - "PAUSE_SCRAPE" -> Pauses scraping (sets internal flag)
    - "RESUME_SCRAPE" -> Resumes scraping
    - ("PROGRESS_EVENT", event) -> Handles progress events from scrapers
    - "GET_CONNECTION_STATUS" -> Returns connection status
    """

    def __init__(
        self,
        scheduler_actor: Any = None,
        discovery_actor: Any = None,
        persistence_actor: Any = None,
        event_handler: Optional[Callable] = None
    ):
        super().__init__()
        self._scheduler_actor = scheduler_actor
        self._discovery_actor = discovery_actor
        self._persistence_actor = persistence_actor
        self._event_handler = event_handler
        self._is_connected = False
        self._is_paused = False
        self._stop_requested = False

    @property
    def is_connected(self) -> bool:
        """Check if bridge is connected to scraper actors."""
        return self._is_connected

    async def start(self):
        """Start the bridge actor and verify connections."""
        await super().start()
        # Consider connected if we have at least the discovery actor
        self._is_connected = (
            self._scheduler_actor is not None or
            self._discovery_actor is not None
        )

    async def handle_message(self, message):
        """Process incoming messages."""
        # String messages
        if isinstance(message, str):
            if message == "STOP_SCRAPE":
                return await self._handle_stop_scrape()
            elif message == "PAUSE_SCRAPE":
                return await self._handle_pause_scrape()
            elif message == "RESUME_SCRAPE":
                return await self._handle_resume_scrape()
            elif message == "GET_CONNECTION_STATUS":
                return {"connected": self._is_connected}

        # Tuple messages
        elif isinstance(message, tuple):
            command = message[0]

            if command == "START_SCRAPE":
                config = message[1]
                return await self._handle_start_scrape(config)
            elif command == "PROGRESS_EVENT":
                event = message[1]
                return await self._handle_progress_event(event)
            elif command == "LOG_EVENT":
                event = message[1]
                return await self._handle_log_event(event)
            elif command == "COMPLETED_EVENT":
                event = message[1]
                return await self._handle_completed_event(event)
            elif command == "FAILED_EVENT":
                event = message[1]
                return await self._handle_failed_event(event)

        return None

    async def _handle_start_scrape(self, config: ScraperConfiguration) -> dict:
        """Handle START_SCRAPE command by forwarding to discovery actor."""
        self._stop_requested = False
        self._is_paused = False

        if self._discovery_actor is None:
            return {"success": False, "error": "Discovery actor not connected"}

        # Translate GUI configuration to discovery actor message
        if config.mode == ScraperMode.TODAY:
            await self._discovery_actor.tell("DISCOVER_TODAY")
        elif config.mode == ScraperMode.SINGLE_DATE:
            target_date = config.single_date or (config.date_range.start if config.date_range else date.today())
            await self._discovery_actor.tell(("DISCOVER_DATE", target_date))
        elif config.mode == ScraperMode.DATE_RANGE:
            if config.date_range:
                await self._discovery_actor.tell((
                    "DISCOVER_RANGE",
                    config.date_range.start,
                    config.date_range.end
                ))
        elif config.mode == ScraperMode.HISTORICAL:
            # Historical mode - could be a large range
            if config.date_range:
                await self._discovery_actor.tell((
                    "DISCOVER_RANGE",
                    config.date_range.start,
                    config.date_range.end
                ))

        return {"success": True}

    async def _handle_stop_scrape(self) -> dict:
        """Handle STOP_SCRAPE command."""
        self._stop_requested = True
        # The scraper actors don't have a built-in stop mechanism,
        # so we set a flag that can be checked
        return {"success": True}

    async def _handle_pause_scrape(self) -> dict:
        """Handle PAUSE_SCRAPE command."""
        self._is_paused = True
        return {"success": True}

    async def _handle_resume_scrape(self) -> dict:
        """Handle RESUME_SCRAPE command."""
        self._is_paused = False
        return {"success": True}

    async def _handle_progress_event(self, event: JobProgressEvent) -> None:
        """Forward progress events to the event handler."""
        if self._event_handler:
            await self._call_event_handler(event)

    async def _handle_log_event(self, event: LogAddedEvent) -> None:
        """Forward log events to the event handler."""
        if self._event_handler:
            await self._call_event_handler(event)

    async def _handle_completed_event(self, event: JobCompletedEvent) -> None:
        """Forward completion events to the event handler."""
        if self._event_handler:
            await self._call_event_handler(event)

    async def _handle_failed_event(self, event: JobFailedEvent) -> None:
        """Forward failure events to the event handler."""
        if self._event_handler:
            await self._call_event_handler(event)

    async def _call_event_handler(self, event: Any) -> None:
        """Call the event handler, handling both sync and async handlers."""
        try:
            if asyncio.iscoroutinefunction(self._event_handler):
                await self._event_handler(event)
            else:
                self._event_handler(event)
        except Exception:
            pass  # Don't let handler errors break the bridge
