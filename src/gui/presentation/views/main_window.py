"""
Main Window

The main application window that assembles all components.
Follows the Mediator pattern to coordinate between components.
"""
import tkinter as tk
from tkinter import ttk, messagebox
from typing import Optional, Callable, Any
import asyncio
import threading

from src.gui.presentation.view_models import (
    JobViewModel,
    ProgressViewModel,
    LogEntryViewModel,
)
from src.gui.presentation.presenters import MainPresenter, JobListPresenter
from .components import (
    StatusPanel,
    ProgressPanel,
    LogPanel,
    ConfigurationPanel,
    ControlPanel,
    JobHistoryPanel,
)


class MainWindow:
    """
    Main application window.

    Coordinates all UI components and connects them to the presenter.
    """

    def __init__(
        self,
        on_start: Optional[Callable] = None,
        on_pause: Optional[Callable] = None,
        on_resume: Optional[Callable] = None,
        on_cancel: Optional[Callable] = None,
        on_close: Optional[Callable] = None
    ):
        """
        Initialize the main window.

        Args:
            on_start: Callback when start button is clicked
            on_pause: Callback when pause button is clicked
            on_resume: Callback when resume button is clicked
            on_cancel: Callback when cancel button is clicked
            on_close: Callback when window is closed
        """
        self._on_start = on_start
        self._on_pause = on_pause
        self._on_resume = on_resume
        self._on_cancel = on_cancel
        self._on_close = on_close

        self._root: Optional[tk.Tk] = None
        self._setup_window()
        self._setup_components()
        self._setup_presenters()

    def _setup_window(self):
        """Setup the main window."""
        self._root = tk.Tk()
        self._root.title("Legal Scraper - DOF Document Collector")
        self._root.geometry("900x700")
        self._root.minsize(800, 600)

        # Handle window close
        self._root.protocol("WM_DELETE_WINDOW", self._handle_close)

        # Configure grid weights for responsive layout
        self._root.grid_columnconfigure(0, weight=1)
        self._root.grid_columnconfigure(1, weight=1)
        self._root.grid_rowconfigure(2, weight=1)

        # Style configuration
        style = ttk.Style()
        style.theme_use('clam')

    def _setup_components(self):
        """Setup all UI components."""
        # Top row: Status and Progress
        top_frame = ttk.Frame(self._root)
        top_frame.grid(row=0, column=0, columnspan=2, sticky='ew', padx=10, pady=5)
        top_frame.grid_columnconfigure(0, weight=1)
        top_frame.grid_columnconfigure(1, weight=1)

        self._status_panel = StatusPanel(top_frame)
        self._status_panel.grid(row=0, column=0, sticky='ew', padx=5)

        self._progress_panel = ProgressPanel(top_frame)
        self._progress_panel.grid(row=0, column=1, sticky='ew', padx=5)

        # Middle row: Configuration and Controls
        config_frame = ttk.Frame(self._root)
        config_frame.grid(row=1, column=0, columnspan=2, sticky='ew', padx=10, pady=5)
        config_frame.grid_columnconfigure(0, weight=1)

        self._config_panel = ConfigurationPanel(config_frame)
        self._config_panel.grid(row=0, column=0, sticky='ew', padx=5)

        self._control_panel = ControlPanel(
            config_frame,
            on_start=self._handle_start,
            on_pause=self._handle_pause,
            on_resume=self._handle_resume,
            on_cancel=self._handle_cancel
        )
        self._control_panel.grid(row=1, column=0, sticky='ew', padx=5, pady=5)

        # Bottom row: Logs and History
        bottom_frame = ttk.Frame(self._root)
        bottom_frame.grid(row=2, column=0, columnspan=2, sticky='nsew', padx=10, pady=5)
        bottom_frame.grid_columnconfigure(0, weight=2)
        bottom_frame.grid_columnconfigure(1, weight=1)
        bottom_frame.grid_rowconfigure(0, weight=1)

        self._log_panel = LogPanel(bottom_frame)
        self._log_panel.grid(row=0, column=0, sticky='nsew', padx=5)

        self._history_panel = JobHistoryPanel(bottom_frame)
        self._history_panel.grid(row=0, column=1, sticky='nsew', padx=5)

        # Status bar
        self._statusbar = ttk.Label(
            self._root,
            text="Ready",
            relief=tk.SUNKEN,
            anchor=tk.W
        )
        self._statusbar.grid(row=3, column=0, columnspan=2, sticky='ew')

    def _setup_presenters(self):
        """Setup presenters for the view."""
        self._main_presenter = MainPresenter(view=self)
        self._history_presenter = JobListPresenter(view=self._history_panel)

    def _handle_start(self):
        """Handle start button click."""
        if self._on_start:
            config = self._get_configuration()
            self._on_start(config)

    def _handle_pause(self):
        """Handle pause button click."""
        if self._on_pause:
            self._on_pause()

    def _handle_resume(self):
        """Handle resume button click."""
        if self._on_resume:
            self._on_resume()

    def _handle_cancel(self):
        """Handle cancel button click."""
        if messagebox.askyesno("Confirm", "Are you sure you want to cancel the current job?"):
            if self._on_cancel:
                self._on_cancel()

    def _handle_close(self):
        """Handle window close."""
        if self._on_close:
            self._on_close()
        self._root.destroy()

    def _get_configuration(self) -> dict:
        """Get current configuration from UI."""
        return {
            'source': self._config_panel.get_source(),
            'mode': self._config_panel.get_mode(),
            'start_date': self._config_panel.get_start_date(),
            'end_date': self._config_panel.get_end_date(),
            'output_directory': self._config_panel.get_output_directory(),
            'rate_limit': self._config_panel.get_rate_limit(),
        }

    # MainViewProtocol implementation

    def update_job_status(self, job_vm: JobViewModel) -> None:
        """Update the job status display."""
        self._status_panel.update_status(job_vm)
        self._control_panel.update_buttons(job_vm)
        self._config_panel.set_enabled(not job_vm.can_pause and not job_vm.can_resume)
        self._statusbar.config(text=f"Status: {job_vm.status_display}")

    def update_progress(self, progress_vm: ProgressViewModel) -> None:
        """Update the progress display."""
        self._progress_panel.update_progress(progress_vm)

    def add_log_entry(self, log_vm: LogEntryViewModel) -> None:
        """Add a log entry to the log display."""
        self._log_panel.add_log_entry(log_vm)

    def clear_logs(self) -> None:
        """Clear all log entries."""
        self._log_panel.clear_logs()

    def set_connection_status(self, connected: bool) -> None:
        """Update connection status indicator."""
        self._status_panel.set_connection_status(connected)

    def add_job_to_history(self, job_vm: JobViewModel) -> None:
        """Add a completed job to history."""
        self._history_panel.add_job_to_history(job_vm)

    def reset_ui(self) -> None:
        """Reset UI to initial state."""
        self._status_panel.update_status(None)
        self._progress_panel.reset()
        self._control_panel.update_buttons(None)
        self._config_panel.set_enabled(True)
        self._statusbar.config(text="Ready")

    def show_error(self, title: str, message: str) -> None:
        """Show error dialog."""
        messagebox.showerror(title, message)

    def show_info(self, title: str, message: str) -> None:
        """Show info dialog."""
        messagebox.showinfo(title, message)

    @property
    def presenter(self) -> MainPresenter:
        """Get the main presenter."""
        return self._main_presenter

    @property
    def history_presenter(self) -> JobListPresenter:
        """Get the history presenter."""
        return self._history_presenter

    def run(self) -> None:
        """Run the main event loop."""
        self._root.mainloop()

    def schedule(self, callback: Callable, delay_ms: int = 0) -> None:
        """Schedule a callback on the main thread."""
        if self._root:
            self._root.after(delay_ms, callback)

    def quit(self) -> None:
        """Quit the application."""
        if self._root:
            self._root.quit()


class AsyncMainWindow(MainWindow):
    """
    Main window with async support.

    Integrates Tkinter's event loop with asyncio.
    """

    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None, **kwargs):
        super().__init__(**kwargs)
        self._loop = loop or asyncio.new_event_loop()
        self._running = False

    def run_async(self) -> None:
        """Run the window with async support."""
        self._running = True
        self._schedule_async_tick()
        self._root.mainloop()

    def _schedule_async_tick(self):
        """Schedule periodic async event processing."""
        if self._running and self._root:
            # Process pending async tasks
            self._loop.run_until_complete(asyncio.sleep(0))
            # Schedule next tick
            self._root.after(10, self._schedule_async_tick)

    def run_coroutine(self, coro) -> Any:
        """Run a coroutine from the GUI thread."""
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future

    def quit(self) -> None:
        """Quit the application."""
        self._running = False
        super().quit()
