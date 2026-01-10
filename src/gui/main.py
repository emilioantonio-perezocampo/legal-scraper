"""
Legal Scraper GUI - Main Entry Point

This module provides the main entry point for the GUI application.
It can be run standalone or integrated with the existing scraper system.
"""
import asyncio
import sys
import os
import threading
from typing import Optional
from datetime import date

# Add src to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.gui.domain.entities import (
    ScraperConfiguration,
    DateRange,
    LogLevel,
)
from src.gui.domain.value_objects import (
    TargetSource,
    OutputFormat,
    ScraperMode,
)
from src.gui.application.services import ScraperService, ConfigurationService
from src.gui.presentation.views.main_window import MainWindow
from src.gui.presentation.view_models import (
    JobViewModel,
    ProgressViewModel,
    LogEntryViewModel,
)
from src.infrastructure.actors.scheduler import SchedulerActor
from src.infrastructure.actors.dof_actor import DofScraperActor
from src.infrastructure.actors.persistence import PersistenceActor
from src.infrastructure.actors.dof_discovery_actor import DofDiscoveryActor


class GuiApplication:
    """
    Main GUI application controller.

    Coordinates between the UI and the scraper service.
    Uses a separate thread for async operations to avoid blocking the UI.
    """

    def __init__(self):
        self._service: Optional[ScraperService] = None
        self._config_service = ConfigurationService()
        self._window: Optional[MainWindow] = None
        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        self._async_thread: Optional[threading.Thread] = None
        self._running = False

    def _setup_async_loop(self):
        """Setup the async event loop in a separate thread."""
        self._async_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._async_loop)

        def run_loop():
            self._async_loop.run_forever()

        self._async_thread = threading.Thread(target=run_loop, daemon=True)
        self._async_thread.start()

    def _run_async(self, coro):
        """Run a coroutine from the main thread."""
        if self._async_loop:
            future = asyncio.run_coroutine_threadsafe(coro, self._async_loop)
            return future

    def _on_state_change(self, state):
        """Handle state changes from the service."""
        if self._window and state.current_job:
            # Schedule UI update on main thread
            job_vm = JobViewModel.from_domain(state.current_job)

            def update_ui():
                self._window.update_job_status(job_vm)
                if state.current_job.progress:
                    progress_vm = ProgressViewModel.from_domain(state.current_job.progress)
                    self._window.update_progress(progress_vm)

            self._window.schedule(update_ui)

    async def _initialize_service(self):
        """Initialize the scraper service."""
        # Initialize backend actors for standalone mode
        self._persistence = PersistenceActor(output_dir="scraped_data")
        self._scraper = DofScraperActor(output_actor=self._persistence)
        self._discovery = DofDiscoveryActor(worker_actor=self._scraper)
        self._scheduler = SchedulerActor()

        # Start backend actors
        await self._persistence.start()
        await self._scraper.start()
        await self._discovery.start()
        await self._scheduler.start()

        # Register worker with scheduler
        await self._scheduler.tell(("REGISTER_WORKER", self._discovery))

        self._service = ScraperService(
            scheduler_actor=self._scheduler,
            discovery_actor=self._discovery,
            persistence_actor=self._persistence,
            state_change_callback=self._on_state_change
        )
        await self._service.start()

        # Add initial log
        await self._service.state_actor.ask((
            "ADD_LOG",
            LogLevel.INFO,
            "GUI initialized and ready (Standalone Mode)",
            "Application"
        ))

    async def _shutdown_service(self):
        """Shutdown the scraper service."""
        if self._service:
            await self._service.stop()
        
        # Stop backend actors if they exist
        if hasattr(self, '_scheduler'):
            await self._scheduler.stop()
        if hasattr(self, '_discovery'):
            await self._discovery.stop()
        if hasattr(self, '_scraper'):
            await self._scraper.stop()
        if hasattr(self, '_persistence'):
            await self._persistence.stop()

    def _handle_start(self, config_dict: dict):
        """Handle start button click."""
        async def start_job():
            try:
                # Build configuration
                mode = config_dict['mode']
                date_range = None

                if mode in (ScraperMode.DATE_RANGE, ScraperMode.HISTORICAL):
                    start_date = config_dict.get('start_date')
                    end_date = config_dict.get('end_date')
                    if start_date and end_date:
                        date_range = DateRange(start=start_date, end=end_date)

                config = ScraperConfiguration(
                    target_source=config_dict['source'],
                    mode=mode,
                    output_format=OutputFormat.JSON,
                    output_directory=config_dict['output_directory'],
                    date_range=date_range,
                    rate_limit_seconds=config_dict.get('rate_limit', 2.0)
                )

                result = await self._service.start_scraping(config)

                if not result.success:
                    self._window.schedule(
                        lambda: self._window.show_error("Error", result.error or "Failed to start")
                    )

            except Exception as e:
                self._window.schedule(
                    lambda: self._window.show_error("Error", str(e))
                )

        self._run_async(start_job())

    def _handle_pause(self):
        """Handle pause button click."""
        async def pause_job():
            result = await self._service.pause_scraping()
            if not result.success:
                self._window.schedule(
                    lambda: self._window.show_error("Error", result.error or "Failed to pause")
                )

        self._run_async(pause_job())

    def _handle_resume(self):
        """Handle resume button click."""
        async def resume_job():
            result = await self._service.resume_scraping()
            if not result.success:
                self._window.schedule(
                    lambda: self._window.show_error("Error", result.error or "Failed to resume")
                )

        self._run_async(resume_job())

    def _handle_cancel(self):
        """Handle cancel button click."""
        async def cancel_job():
            result = await self._service.cancel_scraping()
            if not result.success:
                self._window.schedule(
                    lambda: self._window.show_error("Error", result.error or "Failed to cancel")
                )
            else:
                self._window.schedule(self._window.reset_ui)

        self._run_async(cancel_job())

    def _handle_close(self):
        """Handle window close."""
        self._running = False
        if self._async_loop:
            self._run_async(self._shutdown_service())
            self._async_loop.call_soon_threadsafe(self._async_loop.stop)

    def run(self):
        """Run the GUI application."""
        # Setup async loop in background thread
        self._setup_async_loop()

        # Initialize service
        init_future = self._run_async(self._initialize_service())
        init_future.result(timeout=5)  # Wait for initialization

        # Create and run window
        self._window = MainWindow(
            on_start=self._handle_start,
            on_pause=self._handle_pause,
            on_resume=self._handle_resume,
            on_cancel=self._handle_cancel,
            on_close=self._handle_close
        )

        self._running = True
        self._window.run()


class IntegratedGuiApplication(GuiApplication):
    """
    GUI application integrated with existing scraper actors.

    Use this when you want to connect the GUI to an already-running
    scraper system.
    """

    def __init__(
        self,
        scheduler_actor=None,
        discovery_actor=None,
        persistence_actor=None
    ):
        super().__init__()
        self._scheduler_actor = scheduler_actor
        self._discovery_actor = discovery_actor
        self._persistence_actor = persistence_actor

    async def _initialize_service(self):
        """Initialize with existing actors."""
        self._service = ScraperService(
            scheduler_actor=self._scheduler_actor,
            discovery_actor=self._discovery_actor,
            persistence_actor=self._persistence_actor,
            state_change_callback=self._on_state_change
        )
        await self._service.start()

        # Set connection status
        self._service._bridge_actor._is_connected = True

        await self._service.state_actor.ask((
            "ADD_LOG",
            LogLevel.SUCCESS,
            "Connected to scraper system",
            "Application"
        ))


def main():
    """Main entry point."""
    print("🚀 Starting Legal Scraper GUI...")
    app = GuiApplication()
    app.run()


def main_integrated(scheduler=None, discovery=None, persistence=None):
    """Entry point for integrated mode."""
    print("🚀 Starting Legal Scraper GUI (Integrated Mode)...")
    app = IntegratedGuiApplication(
        scheduler_actor=scheduler,
        discovery_actor=discovery,
        persistence_actor=persistence
    )
    app.run()


if __name__ == "__main__":
    main()
