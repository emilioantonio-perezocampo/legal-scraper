"""
GUI Controller Actor

Handles user actions and orchestrates between state and scraper actors.
Acts as a mediator between the UI and the backend scraper system.
"""
from typing import Optional, Any
import asyncio

from src.infrastructure.actors.base import BaseActor
from src.gui.domain.entities import (
    ScraperJob,
    JobStatus,
    ScraperConfiguration,
    LogLevel,
)
from src.gui.infrastructure.actors.gui_state import GuiStateActor


class GuiControllerActor(BaseActor):
    """
    Actor responsible for handling user actions from the GUI.

    Orchestrates between:
    - GuiStateActor: For state management
    - ScraperBridge: For communication with scraper actors

    Messages:
    - ("START_JOB", config) -> Starts a new scraping job
    - "PAUSE_JOB" -> Pauses the current job
    - "RESUME_JOB" -> Resumes a paused job
    - "CANCEL_JOB" -> Cancels the current job
    - "GET_STATUS" -> Returns current job status

    Returns:
    - dict with "success" (bool) and optionally "error" (str) or "data" (any)
    """

    def __init__(
        self,
        state_actor: GuiStateActor,
        scraper_bridge: Any = None  # Bridge actor for scraper communication
    ):
        super().__init__()
        self._state_actor = state_actor
        self._scraper_bridge = scraper_bridge

    async def handle_message(self, message):
        """Process user action messages."""
        try:
            # Handle tuple messages (command with arguments)
            if isinstance(message, tuple):
                command = message[0]

                if command == "START_JOB":
                    return await self._handle_start_job(message[1])

            # Handle string messages (simple commands)
            elif isinstance(message, str):
                if message == "PAUSE_JOB":
                    return await self._handle_pause_job()
                elif message == "RESUME_JOB":
                    return await self._handle_resume_job()
                elif message == "CANCEL_JOB":
                    return await self._handle_cancel_job()
                elif message == "GET_STATUS":
                    return await self._handle_get_status()

            return {"success": False, "error": "Unknown command"}

        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _handle_start_job(self, config: ScraperConfiguration) -> dict:
        """Handle START_JOB command."""
        # Check if a job is already running
        state = await self._state_actor.ask("GET_STATE")
        if state.current_job is not None and state.current_job.status == JobStatus.RUNNING:
            return {
                "success": False,
                "error": "A job is already running. Please wait or cancel it first."
            }

        # Create new job
        job = await self._state_actor.ask(("CREATE_JOB", config))

        # Add initial log
        await self._state_actor.ask((
            "ADD_LOG",
            LogLevel.INFO,
            f"Job created: {config.target_source.display_name} - {config.mode.value}",
            "Controller"
        ))

        # Start the job (update status)
        await self._state_actor.ask(("UPDATE_JOB_STATUS", JobStatus.RUNNING))

        # Notify scraper bridge to start scraping
        if self._scraper_bridge:
            await self._scraper_bridge.tell(("START_SCRAPE", config))

        await self._state_actor.ask((
            "ADD_LOG",
            LogLevel.SUCCESS,
            "Scraping started",
            "Controller"
        ))

        return {"success": True, "data": {"job_id": job.id}}

    async def _handle_pause_job(self) -> dict:
        """Handle PAUSE_JOB command."""
        state = await self._state_actor.ask("GET_STATE")

        if state.current_job is None:
            return {"success": False, "error": "No job to pause"}

        if state.current_job.status != JobStatus.RUNNING:
            return {"success": False, "error": "Job is not running"}

        await self._state_actor.ask(("UPDATE_JOB_STATUS", JobStatus.PAUSED))

        if self._scraper_bridge:
            await self._scraper_bridge.tell("PAUSE_SCRAPE")

        await self._state_actor.ask((
            "ADD_LOG",
            LogLevel.WARNING,
            "Job paused by user",
            "Controller"
        ))

        return {"success": True}

    async def _handle_resume_job(self) -> dict:
        """Handle RESUME_JOB command."""
        state = await self._state_actor.ask("GET_STATE")

        if state.current_job is None:
            return {"success": False, "error": "No job to resume"}

        if state.current_job.status != JobStatus.PAUSED:
            return {"success": False, "error": "Job is not paused"}

        await self._state_actor.ask(("UPDATE_JOB_STATUS", JobStatus.RUNNING))

        if self._scraper_bridge:
            await self._scraper_bridge.tell("RESUME_SCRAPE")

        await self._state_actor.ask((
            "ADD_LOG",
            LogLevel.SUCCESS,
            "Job resumed",
            "Controller"
        ))

        return {"success": True}

    async def _handle_cancel_job(self) -> dict:
        """Handle CANCEL_JOB command."""
        state = await self._state_actor.ask("GET_STATE")

        if state.current_job is None:
            return {"success": False, "error": "No job to cancel"}

        if state.current_job.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            return {"success": False, "error": "Job is already finished"}

        await self._state_actor.ask((
            "ADD_LOG",
            LogLevel.WARNING,
            "Job cancelled by user",
            "Controller"
        ))

        await self._state_actor.ask(("UPDATE_JOB_STATUS", JobStatus.CANCELLED))

        if self._scraper_bridge:
            await self._scraper_bridge.tell("STOP_SCRAPE")

        return {"success": True}

    async def _handle_get_status(self) -> dict:
        """Handle GET_STATUS command."""
        state = await self._state_actor.ask("GET_STATE")

        if state.current_job is None:
            return {
                "success": True,
                "data": {
                    "status": "idle",
                    "job": None
                }
            }

        return {
            "success": True,
            "data": {
                "status": state.current_job.status.value,
                "job_id": state.current_job.id,
                "progress": state.current_job.progress
            }
        }
