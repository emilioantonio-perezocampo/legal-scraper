"""
GUI State Actor

Manages the application state in an actor-based pattern.
All state mutations go through message passing, ensuring
thread-safe access in async contexts.
"""
from dataclasses import dataclass, field, replace
from typing import List, Optional, Callable, Any
import asyncio

from src.infrastructure.actors.base import BaseActor
from src.gui.domain.entities import (
    ScraperJob,
    JobStatus,
    JobProgress,
    ScraperConfiguration,
    LogLevel,
)


@dataclass(frozen=True)
class GuiState:
    """
    Immutable state container for the GUI.

    Represents a snapshot of the application state at a point in time.
    """
    current_job: Optional[ScraperJob] = None
    job_history: tuple = field(default_factory=tuple)
    is_connected: bool = False
    error_message: Optional[str] = None


class GuiStateActor(BaseActor):
    """
    Actor responsible for managing GUI application state.

    Follows the Actor Model pattern:
    - State is encapsulated and only modified via messages
    - Supports both 'tell' (fire-and-forget) and 'ask' (request-response) patterns
    - Thread-safe for async operations

    Messages:
    - "GET_STATE" -> Returns current GuiState
    - ("CREATE_JOB", config) -> Creates new job, returns ScraperJob
    - ("UPDATE_JOB_STATUS", status) -> Updates job status, returns ScraperJob
    - ("UPDATE_PROGRESS", progress) -> Updates job progress, returns ScraperJob
    - ("ADD_LOG", level, message, source) -> Adds log entry, returns ScraperJob
    - ("SET_CONNECTED", bool) -> Sets connection status
    - ("SET_ERROR", message) -> Sets error message
    """

    def __init__(self, state_change_callback: Optional[Callable[[GuiState], Any]] = None):
        super().__init__()
        self._state = GuiState()
        self._state_change_callback = state_change_callback

    @property
    def state(self) -> GuiState:
        """Read-only access to current state (for debugging/testing)."""
        return self._state

    def _notify_state_change(self):
        """Notify subscribers of state changes."""
        if self._state_change_callback:
            try:
                self._state_change_callback(self._state)
            except Exception:
                pass  # Don't let callback errors break the actor

    async def handle_message(self, message):
        """Process incoming messages and update state accordingly."""
        # Simple string messages
        if message == "GET_STATE":
            return self._state

        # Tuple messages
        if isinstance(message, tuple):
            command = message[0]

            if command == "CREATE_JOB":
                config = message[1]
                job = ScraperJob(configuration=config)
                self._state = replace(self._state, current_job=job)
                self._notify_state_change()
                return job

            elif command == "UPDATE_JOB_STATUS":
                new_status = message[1]
                if self._state.current_job is None:
                    return None

                current_job = self._state.current_job

                if new_status == JobStatus.RUNNING:
                    if current_job.status == JobStatus.PAUSED:
                        updated_job = current_job.resume()
                    else:
                        updated_job = current_job.start()
                elif new_status == JobStatus.PAUSED:
                    updated_job = current_job.pause()
                elif new_status == JobStatus.COMPLETED:
                    updated_job = current_job.complete()
                    # Add to history and clear current
                    new_history = self._state.job_history + (updated_job,)
                    self._state = replace(
                        self._state,
                        current_job=None,
                        job_history=new_history
                    )
                    self._notify_state_change()
                    return updated_job
                elif new_status == JobStatus.FAILED:
                    error_msg = message[2] if len(message) > 2 else "Unknown error"
                    updated_job = current_job.fail(error_msg)
                    new_history = self._state.job_history + (updated_job,)
                    self._state = replace(
                        self._state,
                        current_job=None,
                        job_history=new_history
                    )
                    self._notify_state_change()
                    return updated_job
                elif new_status == JobStatus.CANCELLED:
                    updated_job = current_job.cancel()
                    new_history = self._state.job_history + (updated_job,)
                    self._state = replace(
                        self._state,
                        current_job=None,
                        job_history=new_history
                    )
                    self._notify_state_change()
                    return updated_job
                else:
                    return current_job

                self._state = replace(self._state, current_job=updated_job)
                self._notify_state_change()
                return updated_job

            elif command == "UPDATE_PROGRESS":
                progress = message[1]
                if self._state.current_job is None:
                    return None
                updated_job = self._state.current_job.update_progress(progress)
                self._state = replace(self._state, current_job=updated_job)
                self._notify_state_change()
                return updated_job

            elif command == "ADD_LOG":
                level, msg, source = message[1], message[2], message[3]
                if self._state.current_job is None:
                    return None
                updated_job = self._state.current_job.add_log(level, msg, source)
                self._state = replace(self._state, current_job=updated_job)
                self._notify_state_change()
                return updated_job

            elif command == "SET_CONNECTED":
                is_connected = message[1]
                self._state = replace(self._state, is_connected=is_connected)
                self._notify_state_change()
                return None

            elif command == "SET_ERROR":
                error_message = message[1]
                self._state = replace(self._state, error_message=error_message)
                self._notify_state_change()
                return None

        return None
