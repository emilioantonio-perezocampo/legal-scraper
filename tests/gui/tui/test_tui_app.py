"""
RED Phase Tests: Terminal TUI Application

Tests for the Textual-based terminal user interface.
"""
import pytest
from datetime import date
from unittest.mock import MagicMock, AsyncMock

from src.gui.tui.app import ScraperTUI, StatusWidget, ProgressWidget, LogWidget
from src.gui.domain.entities import (
    ScraperJob,
    JobStatus,
    JobProgress,
    ScraperConfiguration,
    LogLevel,
    LogEntry,
)
from src.gui.domain.value_objects import (
    TargetSource,
    OutputFormat,
    ScraperMode,
)


class TestScraperTUI:
    """Tests for the main TUI application."""

    def test_tui_app_creation(self):
        """TUI app should be creatable without errors."""
        app = ScraperTUI()
        assert app is not None
        assert "Legal Scraper" in app.title or app.title == "ScraperTUI"

    def test_tui_app_has_required_widgets(self):
        """TUI app should define required widget classes."""
        # Verify widget classes exist and are importable
        assert StatusWidget is not None
        assert ProgressWidget is not None
        assert LogWidget is not None


class TestStatusWidget:
    """Tests for the status display widget."""

    def test_status_widget_creation(self):
        """StatusWidget should be creatable."""
        widget = StatusWidget()
        assert widget is not None

    def test_status_widget_default_state(self):
        """StatusWidget should show idle by default."""
        widget = StatusWidget()
        assert widget.status == "idle"

    def test_status_widget_update(self):
        """StatusWidget should update on status change."""
        widget = StatusWidget()
        widget.update_status("running")
        assert widget.status == "running"


class TestProgressWidget:
    """Tests for the progress display widget."""

    def test_progress_widget_creation(self):
        """ProgressWidget should be creatable."""
        widget = ProgressWidget()
        assert widget is not None

    def test_progress_widget_default_state(self):
        """ProgressWidget should show 0% by default."""
        widget = ProgressWidget()
        assert widget.percentage == 0.0

    def test_progress_widget_update(self):
        """ProgressWidget should update progress."""
        widget = ProgressWidget()
        progress = JobProgress(
            total_items=100,
            processed_items=50,
            successful_items=48,
            failed_items=2
        )
        widget.update_progress(progress)
        assert widget.percentage == 50.0
        assert widget.success_count == 48
        assert widget.failure_count == 2


class TestLogWidget:
    """Tests for the log display widget."""

    def test_log_widget_creation(self):
        """LogWidget should be creatable."""
        widget = LogWidget()
        assert widget is not None

    def test_log_widget_add_entry(self):
        """LogWidget should accept log entries."""
        widget = LogWidget()
        entry = LogEntry(
            level=LogLevel.INFO,
            message="Test message",
            source="Test"
        )
        widget.add_log(entry)
        assert len(widget.logs) == 1

    def test_log_widget_clear(self):
        """LogWidget should support clearing logs."""
        widget = LogWidget()
        entry = LogEntry(
            level=LogLevel.INFO,
            message="Test message",
            source="Test"
        )
        widget.add_log(entry)
        widget.clear_logs()
        assert len(widget.logs) == 0


class TestTUICommands:
    """Tests for TUI command handling."""

    def test_start_command_available(self):
        """TUI should have start command."""
        app = ScraperTUI()
        assert hasattr(app, 'action_start')

    def test_pause_command_available(self):
        """TUI should have pause command."""
        app = ScraperTUI()
        assert hasattr(app, 'action_pause')

    def test_resume_command_available(self):
        """TUI should have resume command."""
        app = ScraperTUI()
        assert hasattr(app, 'action_resume')

    def test_cancel_command_available(self):
        """TUI should have cancel command."""
        app = ScraperTUI()
        assert hasattr(app, 'action_cancel')

    def test_quit_command_available(self):
        """TUI should have quit command."""
        app = ScraperTUI()
        assert hasattr(app, 'action_quit')


class TestTUIKeyBindings:
    """Tests for TUI keyboard shortcuts."""

    def test_keybindings_defined(self):
        """TUI should define keyboard bindings."""
        app = ScraperTUI()
        bindings = app.BINDINGS
        assert len(bindings) > 0

    def test_quit_keybinding(self):
        """TUI should have quit keybinding (q)."""
        app = ScraperTUI()
        binding_keys = [b.key for b in app.BINDINGS]
        assert "q" in binding_keys or "Q" in binding_keys
