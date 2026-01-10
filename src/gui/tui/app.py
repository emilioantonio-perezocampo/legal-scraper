"""
Terminal TUI Application using Textual

Provides a rich terminal interface for the legal scraper.
Works over SSH on headless servers like Hetzner VMs.
"""
import asyncio
from datetime import date
from typing import List, Optional
from dataclasses import dataclass, field

from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import (
    Header,
    Footer,
    Static,
    Button,
    Input,
    Select,
    ProgressBar,
    RichLog,
    Label,
)
from textual.binding import Binding
from textual.reactive import reactive
from rich.text import Text
from rich.panel import Panel
from rich.table import Table

from src.gui.domain.entities import (
    ScraperConfiguration,
    JobProgress,
    LogLevel,
    LogEntry,
    DateRange,
)
from src.gui.domain.value_objects import (
    TargetSource,
    OutputFormat,
    ScraperMode,
)
from src.gui.application.services import ScraperService, ConfigurationService


class StatusWidget(Static):
    """Widget displaying current job status."""

    status = reactive("idle")
    source = reactive("")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._status = "idle"

    def update_status(self, new_status: str, source: str = ""):
        """Update the status display."""
        self._status = new_status
        self.status = new_status
        self.source = source
        self.refresh()

    def render(self) -> Panel:
        """Render the status widget."""
        status_colors = {
            "idle": "dim",
            "running": "green bold",
            "paused": "yellow",
            "completed": "blue",
            "failed": "red bold",
            "cancelled": "dim"
        }
        style = status_colors.get(self.status, "white")
        status_text = Text(self.status.upper(), style=style)

        content = Table.grid(padding=1)
        content.add_column()
        content.add_row(status_text)
        if self.source:
            content.add_row(Text(self.source, style="dim"))

        return Panel(content, title="Status", border_style="blue")


class ProgressWidget(Static):
    """Widget displaying job progress."""

    percentage = reactive(0.0)
    items_processed = reactive(0)
    items_total = reactive(0)
    success_count = reactive(0)
    failure_count = reactive(0)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._percentage = 0.0
        self._success_count = 0
        self._failure_count = 0

    def update_progress(self, progress: JobProgress):
        """Update progress from domain object."""
        self._percentage = progress.percentage
        self._success_count = progress.successful_items
        self._failure_count = progress.failed_items
        self.percentage = progress.percentage
        self.items_processed = progress.processed_items
        self.items_total = progress.total_items
        self.success_count = progress.successful_items
        self.failure_count = progress.failed_items
        self.refresh()

    def render(self) -> Panel:
        """Render the progress widget."""
        content = Table.grid(padding=1)
        content.add_column(justify="left")
        content.add_column(justify="right")

        # Progress bar representation
        bar_width = 30
        filled = int((self.percentage / 100) * bar_width)
        bar = "█" * filled + "░" * (bar_width - filled)

        content.add_row(
            Text(f"[{bar}]", style="green"),
            Text(f"{self.percentage:.1f}%", style="bold")
        )
        content.add_row(
            Text("Items:", style="dim"),
            Text(f"{self.items_processed}/{self.items_total}")
        )
        content.add_row(
            Text("Success:", style="green"),
            Text(str(self.success_count), style="green")
        )
        content.add_row(
            Text("Failed:", style="red"),
            Text(str(self.failure_count), style="red")
        )

        return Panel(content, title="Progress", border_style="green")


class LogWidget(Static):
    """Widget displaying log entries."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._logs: List[LogEntry] = []

    @property
    def logs(self) -> List[LogEntry]:
        return self._logs

    def add_log(self, entry: LogEntry):
        """Add a log entry."""
        self._logs.append(entry)
        # Keep only last 100 entries
        if len(self._logs) > 100:
            self._logs = self._logs[-100:]
        self.refresh()

    def clear_logs(self):
        """Clear all logs."""
        self._logs.clear()
        self.refresh()

    def render(self) -> Panel:
        """Render the log widget."""
        level_colors = {
            LogLevel.DEBUG: "dim",
            LogLevel.INFO: "blue",
            LogLevel.WARNING: "yellow",
            LogLevel.ERROR: "red",
            LogLevel.SUCCESS: "green",
        }

        content = Table.grid(padding=0)
        content.add_column(width=10)
        content.add_column(width=10)
        content.add_column()

        # Show last 15 entries
        for entry in self._logs[-15:]:
            time_str = entry.timestamp.strftime("%H:%M:%S")
            style = level_colors.get(entry.level, "white")
            content.add_row(
                Text(time_str, style="dim"),
                Text(entry.level.value.upper(), style=style),
                Text(f"{entry.source}: {entry.message}")
            )

        if not self._logs:
            content.add_row(Text("No logs yet...", style="dim"))

        return Panel(content, title="Logs", border_style="cyan", height=18)


class ConfigWidget(Static):
    """Widget for configuration input."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._mode = "today"
        self._output_dir = "scraped_data"
        self._start_date = str(date.today())
        self._end_date = str(date.today())

    def compose(self) -> ComposeResult:
        """Compose the configuration form."""
        yield Label("Mode:")
        yield Select(
            [(m.value, m.value) for m in ScraperMode],
            value="today",
            id="mode_select"
        )
        yield Label("Output Directory:")
        yield Input(value="scraped_data", id="output_dir")
        yield Label("Start Date (YYYY-MM-DD):")
        yield Input(value=str(date.today()), id="start_date")
        yield Label("End Date (YYYY-MM-DD):")
        yield Input(value=str(date.today()), id="end_date")


class ScraperTUI(App):
    """
    Main Terminal TUI Application.

    Provides a rich terminal interface for controlling the legal scraper.
    Works over SSH on headless servers.
    """

    CSS = """
    Screen {
        layout: grid;
        grid-size: 2 3;
        grid-gutter: 1;
    }

    #status {
        column-span: 1;
        height: auto;
    }

    #progress {
        column-span: 1;
        height: auto;
    }

    #config {
        column-span: 1;
        row-span: 1;
    }

    #controls {
        column-span: 1;
        height: auto;
        align: center middle;
    }

    #logs {
        column-span: 2;
        height: 100%;
    }

    Button {
        margin: 1;
    }

    .btn-start {
        background: green;
    }

    .btn-pause {
        background: yellow;
        color: black;
    }

    .btn-cancel {
        background: red;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("s", "start", "Start"),
        Binding("p", "pause", "Pause"),
        Binding("r", "resume", "Resume"),
        Binding("c", "cancel", "Cancel"),
        Binding("l", "clear_logs", "Clear Logs"),
    ]

    def __init__(self, service: Optional[ScraperService] = None, **kwargs):
        super().__init__(**kwargs)
        self._service = service
        self._config_service = ConfigurationService()

    def compose(self) -> ComposeResult:
        """Compose the application layout."""
        yield Header()

        yield StatusWidget(id="status")
        yield ProgressWidget(id="progress")

        with Vertical(id="config"):
            yield Label("Configuration", classes="title")
            yield Label("Mode:")
            yield Select(
                [(m.value.replace("_", " ").title(), m.value) for m in ScraperMode],
                value="today",
                id="mode_select"
            )
            yield Label("Output Directory:")
            yield Input(value="scraped_data", id="output_dir", placeholder="Output directory")
            yield Label("Start Date:")
            yield Input(value=str(date.today()), id="start_date", placeholder="YYYY-MM-DD")
            yield Label("End Date:")
            yield Input(value=str(date.today()), id="end_date", placeholder="YYYY-MM-DD")

        with Horizontal(id="controls"):
            yield Button("▶ Start", id="btn_start", variant="success")
            yield Button("⏸ Pause", id="btn_pause", variant="warning", disabled=True)
            yield Button("▶ Resume", id="btn_resume", variant="primary", disabled=True)
            yield Button("⏹ Cancel", id="btn_cancel", variant="error", disabled=True)

        yield LogWidget(id="logs")

        yield Footer()

    async def on_mount(self):
        """Initialize when app mounts."""
        if self._service is None:
            self._service = ScraperService(state_change_callback=self._on_state_change)
            await self._service.start()

        self._add_log(LogLevel.INFO, "TUI initialized", "System")

    def _on_state_change(self, state):
        """Handle state changes from service."""
        if state.current_job:
            status_widget = self.query_one("#status", StatusWidget)
            status_widget.update_status(
                state.current_job.status.value,
                state.current_job.configuration.target_source.display_name
            )

            if state.current_job.progress:
                progress_widget = self.query_one("#progress", ProgressWidget)
                progress_widget.update_progress(state.current_job.progress)

            # Update button states
            self._update_buttons(state.current_job.status.value)

    def _update_buttons(self, status: str):
        """Update button states based on job status."""
        btn_start = self.query_one("#btn_start", Button)
        btn_pause = self.query_one("#btn_pause", Button)
        btn_resume = self.query_one("#btn_resume", Button)
        btn_cancel = self.query_one("#btn_cancel", Button)

        is_running = status == "running"
        is_paused = status == "paused"
        is_idle = status in ("idle", "completed", "failed", "cancelled")

        btn_start.disabled = not is_idle
        btn_pause.disabled = not is_running
        btn_resume.disabled = not is_paused
        btn_cancel.disabled = is_idle

    def _add_log(self, level: LogLevel, message: str, source: str):
        """Add a log entry to the log widget."""
        entry = LogEntry(level=level, message=message, source=source)
        log_widget = self.query_one("#logs", LogWidget)
        log_widget.add_log(entry)

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses."""
        button_id = event.button.id

        if button_id == "btn_start":
            await self.action_start()
        elif button_id == "btn_pause":
            await self.action_pause()
        elif button_id == "btn_resume":
            await self.action_resume()
        elif button_id == "btn_cancel":
            await self.action_cancel()

    async def action_start(self):
        """Start scraping."""
        try:
            mode_select = self.query_one("#mode_select", Select)
            output_input = self.query_one("#output_dir", Input)
            start_input = self.query_one("#start_date", Input)
            end_input = self.query_one("#end_date", Input)

            mode = ScraperMode(mode_select.value)
            output_dir = output_input.value or "scraped_data"

            date_range = None
            if mode in (ScraperMode.DATE_RANGE, ScraperMode.HISTORICAL):
                try:
                    start = date.fromisoformat(start_input.value)
                    end = date.fromisoformat(end_input.value)
                    date_range = DateRange(start=start, end=end)
                except ValueError:
                    self._add_log(LogLevel.ERROR, "Invalid date format", "UI")
                    return

            config = ScraperConfiguration(
                target_source=TargetSource.DOF,
                mode=mode,
                output_format=OutputFormat.JSON,
                output_directory=output_dir,
                date_range=date_range
            )

            result = await self._service.start_scraping(config)

            if result.success:
                self._add_log(LogLevel.SUCCESS, f"Job started: {result.job_id}", "UI")
            else:
                self._add_log(LogLevel.ERROR, result.error or "Failed to start", "UI")

        except Exception as e:
            self._add_log(LogLevel.ERROR, str(e), "UI")

    async def action_pause(self):
        """Pause scraping."""
        result = await self._service.pause_scraping()
        if result.success:
            self._add_log(LogLevel.WARNING, "Job paused", "UI")
        else:
            self._add_log(LogLevel.ERROR, result.error or "Failed to pause", "UI")

    async def action_resume(self):
        """Resume scraping."""
        result = await self._service.resume_scraping()
        if result.success:
            self._add_log(LogLevel.SUCCESS, "Job resumed", "UI")
        else:
            self._add_log(LogLevel.ERROR, result.error or "Failed to resume", "UI")

    async def action_cancel(self):
        """Cancel scraping."""
        result = await self._service.cancel_scraping()
        if result.success:
            self._add_log(LogLevel.WARNING, "Job cancelled", "UI")
            self._update_buttons("idle")
        else:
            self._add_log(LogLevel.ERROR, result.error or "Failed to cancel", "UI")

    def action_clear_logs(self):
        """Clear the log display."""
        log_widget = self.query_one("#logs", LogWidget)
        log_widget.clear_logs()

    async def action_quit(self):
        """Quit the application."""
        if self._service:
            await self._service.stop()
        self.exit()


def run_tui():
    """Run the TUI application."""
    app = ScraperTUI()
    app.run()


if __name__ == "__main__":
    run_tui()
