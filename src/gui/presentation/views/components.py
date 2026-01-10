"""
GUI View Components

Reusable Tkinter components for the GUI.
Each component follows the Single Responsibility Principle.
"""
import tkinter as tk
from tkinter import ttk
from typing import Optional, Callable, List
from datetime import date

from src.gui.presentation.view_models import (
    JobViewModel,
    ProgressViewModel,
    LogEntryViewModel,
    ConfigurationViewModel,
)
from src.gui.domain.value_objects import TargetSource, ScraperMode, OutputFormat


# Color scheme
COLORS = {
    'gray': '#6B7280',
    'green': '#10B981',
    'blue': '#3B82F6',
    'orange': '#F59E0B',
    'red': '#EF4444',
    'yellow': '#FCD34D',
    'bg_dark': '#1F2937',
    'bg_light': '#F3F4F6',
    'text_dark': '#111827',
    'text_light': '#F9FAFB',
}


class StatusPanel(ttk.LabelFrame):
    """Panel displaying current job status."""

    def __init__(self, parent: tk.Widget, **kwargs):
        super().__init__(parent, text="Status", **kwargs)
        self._setup_ui()

    def _setup_ui(self):
        # Status label
        self._status_var = tk.StringVar(value="Idle")
        self._status_label = ttk.Label(
            self,
            textvariable=self._status_var,
            font=('Helvetica', 16, 'bold')
        )
        self._status_label.pack(pady=10)

        # Source info
        self._source_var = tk.StringVar(value="")
        self._source_label = ttk.Label(self, textvariable=self._source_var)
        self._source_label.pack(pady=5)

        # Connection indicator
        self._connection_frame = ttk.Frame(self)
        self._connection_frame.pack(pady=5)

        self._connection_indicator = tk.Canvas(
            self._connection_frame,
            width=12,
            height=12,
            highlightthickness=0
        )
        self._connection_indicator.pack(side=tk.LEFT, padx=5)
        self._connection_indicator.create_oval(2, 2, 10, 10, fill='gray', tags='indicator')

        self._connection_label = ttk.Label(
            self._connection_frame,
            text="Disconnected"
        )
        self._connection_label.pack(side=tk.LEFT)

    def update_status(self, job_vm: Optional[JobViewModel]) -> None:
        """Update status display from view model."""
        if job_vm is None:
            self._status_var.set("Idle")
            self._source_var.set("")
        else:
            self._status_var.set(job_vm.status_display)
            self._source_var.set(job_vm.source_display)

    def set_connection_status(self, connected: bool) -> None:
        """Update connection indicator."""
        color = COLORS['green'] if connected else COLORS['gray']
        text = "Connected" if connected else "Disconnected"
        self._connection_indicator.itemconfig('indicator', fill=color)
        self._connection_label.config(text=text)


class ProgressPanel(ttk.LabelFrame):
    """Panel displaying job progress."""

    def __init__(self, parent: tk.Widget, **kwargs):
        super().__init__(parent, text="Progress", **kwargs)
        self._setup_ui()

    def _setup_ui(self):
        # Progress bar
        self._progress_var = tk.DoubleVar(value=0)
        self._progress_bar = ttk.Progressbar(
            self,
            variable=self._progress_var,
            maximum=100,
            length=300
        )
        self._progress_bar.pack(pady=10, padx=10, fill=tk.X)

        # Progress text
        self._progress_text_var = tk.StringVar(value="0% (0/0)")
        self._progress_label = ttk.Label(
            self,
            textvariable=self._progress_text_var
        )
        self._progress_label.pack(pady=5)

        # Success/Failure counts
        self._counts_frame = ttk.Frame(self)
        self._counts_frame.pack(pady=5, fill=tk.X)

        self._success_var = tk.StringVar(value="Success: 0")
        ttk.Label(
            self._counts_frame,
            textvariable=self._success_var,
            foreground=COLORS['green']
        ).pack(side=tk.LEFT, padx=20)

        self._failure_var = tk.StringVar(value="Failed: 0")
        ttk.Label(
            self._counts_frame,
            textvariable=self._failure_var,
            foreground=COLORS['red']
        ).pack(side=tk.RIGHT, padx=20)

    def update_progress(self, progress_vm: ProgressViewModel) -> None:
        """Update progress display from view model."""
        self._progress_var.set(progress_vm.percentage)
        self._progress_text_var.set(
            f"{progress_vm.percentage_display} ({progress_vm.items_display})"
        )
        self._success_var.set(f"Success: {progress_vm.success_count}")
        self._failure_var.set(f"Failed: {progress_vm.failure_count}")

    def reset(self) -> None:
        """Reset progress to zero."""
        self._progress_var.set(0)
        self._progress_text_var.set("0% (0/0)")
        self._success_var.set("Success: 0")
        self._failure_var.set("Failed: 0")


class LogPanel(ttk.LabelFrame):
    """Panel displaying log entries."""

    def __init__(self, parent: tk.Widget, **kwargs):
        super().__init__(parent, text="Logs", **kwargs)
        self._setup_ui()

    def _setup_ui(self):
        # Create text widget with scrollbar
        self._log_frame = ttk.Frame(self)
        self._log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self._log_text = tk.Text(
            self._log_frame,
            height=10,
            wrap=tk.WORD,
            state=tk.DISABLED,
            font=('Consolas', 9)
        )
        self._log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self._scrollbar = ttk.Scrollbar(
            self._log_frame,
            orient=tk.VERTICAL,
            command=self._log_text.yview
        )
        self._scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self._log_text.config(yscrollcommand=self._scrollbar.set)

        # Configure tags for colors
        self._log_text.tag_configure('DEBUG', foreground=COLORS['gray'])
        self._log_text.tag_configure('INFO', foreground=COLORS['blue'])
        self._log_text.tag_configure('WARNING', foreground=COLORS['orange'])
        self._log_text.tag_configure('ERROR', foreground=COLORS['red'])
        self._log_text.tag_configure('SUCCESS', foreground=COLORS['green'])

        # Clear button
        self._clear_btn = ttk.Button(
            self,
            text="Clear Logs",
            command=self.clear_logs
        )
        self._clear_btn.pack(pady=5)

    def add_log_entry(self, log_vm: LogEntryViewModel) -> None:
        """Add a log entry to the display."""
        self._log_text.config(state=tk.NORMAL)

        log_line = f"{log_vm.timestamp_display} [{log_vm.level_display}] {log_vm.source}: {log_vm.message}\n"
        self._log_text.insert(tk.END, log_line, log_vm.level_display)

        self._log_text.see(tk.END)
        self._log_text.config(state=tk.DISABLED)

    def clear_logs(self) -> None:
        """Clear all log entries."""
        self._log_text.config(state=tk.NORMAL)
        self._log_text.delete(1.0, tk.END)
        self._log_text.config(state=tk.DISABLED)


class ConfigurationPanel(ttk.LabelFrame):
    """Panel for configuring scraping options."""

    def __init__(
        self,
        parent: tk.Widget,
        on_config_changed: Optional[Callable] = None,
        **kwargs
    ):
        super().__init__(parent, text="Configuration", **kwargs)
        self._on_config_changed = on_config_changed
        self._setup_ui()

    def _setup_ui(self):
        # Source selection
        source_frame = ttk.Frame(self)
        source_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(source_frame, text="Source:").pack(side=tk.LEFT)
        self._source_var = tk.StringVar(value=TargetSource.DOF.value)
        self._source_combo = ttk.Combobox(
            source_frame,
            textvariable=self._source_var,
            values=[s.value for s in TargetSource],
            state='readonly',
            width=30
        )
        self._source_combo.pack(side=tk.LEFT, padx=10)

        # Mode selection
        mode_frame = ttk.Frame(self)
        mode_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(mode_frame, text="Mode:").pack(side=tk.LEFT)
        self._mode_var = tk.StringVar(value=ScraperMode.TODAY.value)
        self._mode_combo = ttk.Combobox(
            mode_frame,
            textvariable=self._mode_var,
            values=[m.value for m in ScraperMode],
            state='readonly',
            width=30
        )
        self._mode_combo.pack(side=tk.LEFT, padx=10)
        self._mode_combo.bind('<<ComboboxSelected>>', self._on_mode_changed)

        # Date range frame (hidden by default)
        self._date_frame = ttk.Frame(self)
        self._date_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(self._date_frame, text="Start Date:").pack(side=tk.LEFT)
        self._start_date_var = tk.StringVar(value=str(date.today()))
        self._start_date_entry = ttk.Entry(
            self._date_frame,
            textvariable=self._start_date_var,
            width=12
        )
        self._start_date_entry.pack(side=tk.LEFT, padx=5)

        ttk.Label(self._date_frame, text="End Date:").pack(side=tk.LEFT, padx=(10, 0))
        self._end_date_var = tk.StringVar(value=str(date.today()))
        self._end_date_entry = ttk.Entry(
            self._date_frame,
            textvariable=self._end_date_var,
            width=12
        )
        self._end_date_entry.pack(side=tk.LEFT, padx=5)

        # Output directory
        output_frame = ttk.Frame(self)
        output_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(output_frame, text="Output Dir:").pack(side=tk.LEFT)
        self._output_var = tk.StringVar(value="scraped_data")
        self._output_entry = ttk.Entry(
            output_frame,
            textvariable=self._output_var,
            width=30
        )
        self._output_entry.pack(side=tk.LEFT, padx=10)

        # Rate limit
        rate_frame = ttk.Frame(self)
        rate_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(rate_frame, text="Rate Limit (sec):").pack(side=tk.LEFT)
        self._rate_var = tk.StringVar(value="2.0")
        self._rate_entry = ttk.Entry(
            rate_frame,
            textvariable=self._rate_var,
            width=8
        )
        self._rate_entry.pack(side=tk.LEFT, padx=10)

        # Initially hide date fields based on mode
        self._update_date_visibility()

    def _on_mode_changed(self, event=None):
        """Handle mode change."""
        self._update_date_visibility()
        if self._on_config_changed:
            self._on_config_changed()

    def _update_date_visibility(self):
        """Show/hide date fields based on mode."""
        mode = self._mode_var.get()
        if mode in ('range', 'historical', 'single'):
            self._date_frame.pack(fill=tk.X, padx=10, pady=5)
        else:
            self._date_frame.pack_forget()

    def get_source(self) -> TargetSource:
        """Get selected source."""
        return TargetSource(self._source_var.get())

    def get_mode(self) -> ScraperMode:
        """Get selected mode."""
        return ScraperMode(self._mode_var.get())

    def get_start_date(self) -> Optional[date]:
        """Get start date."""
        try:
            return date.fromisoformat(self._start_date_var.get())
        except ValueError:
            return None

    def get_end_date(self) -> Optional[date]:
        """Get end date."""
        try:
            return date.fromisoformat(self._end_date_var.get())
        except ValueError:
            return None

    def get_output_directory(self) -> str:
        """Get output directory."""
        return self._output_var.get()

    def get_rate_limit(self) -> float:
        """Get rate limit in seconds."""
        try:
            return float(self._rate_var.get())
        except ValueError:
            return 2.0

    def set_enabled(self, enabled: bool) -> None:
        """Enable or disable configuration inputs."""
        state = 'normal' if enabled else 'disabled'
        readonly_state = 'readonly' if enabled else 'disabled'

        self._source_combo.config(state=readonly_state)
        self._mode_combo.config(state=readonly_state)
        self._start_date_entry.config(state=state)
        self._end_date_entry.config(state=state)
        self._output_entry.config(state=state)
        self._rate_entry.config(state=state)


class ControlPanel(ttk.Frame):
    """Panel with control buttons."""

    def __init__(
        self,
        parent: tk.Widget,
        on_start: Optional[Callable] = None,
        on_pause: Optional[Callable] = None,
        on_resume: Optional[Callable] = None,
        on_cancel: Optional[Callable] = None,
        **kwargs
    ):
        super().__init__(parent, **kwargs)
        self._on_start = on_start
        self._on_pause = on_pause
        self._on_resume = on_resume
        self._on_cancel = on_cancel
        self._setup_ui()

    def _setup_ui(self):
        # Button frame
        btn_frame = ttk.Frame(self)
        btn_frame.pack(pady=10)

        self._start_btn = ttk.Button(
            btn_frame,
            text="▶ Start",
            command=self._handle_start,
            width=12
        )
        self._start_btn.pack(side=tk.LEFT, padx=5)

        self._pause_btn = ttk.Button(
            btn_frame,
            text="⏸ Pause",
            command=self._handle_pause,
            width=12,
            state=tk.DISABLED
        )
        self._pause_btn.pack(side=tk.LEFT, padx=5)

        self._resume_btn = ttk.Button(
            btn_frame,
            text="▶ Resume",
            command=self._handle_resume,
            width=12,
            state=tk.DISABLED
        )
        self._resume_btn.pack(side=tk.LEFT, padx=5)

        self._cancel_btn = ttk.Button(
            btn_frame,
            text="⏹ Cancel",
            command=self._handle_cancel,
            width=12,
            state=tk.DISABLED
        )
        self._cancel_btn.pack(side=tk.LEFT, padx=5)

    def _handle_start(self):
        if self._on_start:
            self._on_start()

    def _handle_pause(self):
        if self._on_pause:
            self._on_pause()

    def _handle_resume(self):
        if self._on_resume:
            self._on_resume()

    def _handle_cancel(self):
        if self._on_cancel:
            self._on_cancel()

    def update_buttons(self, job_vm: Optional[JobViewModel]) -> None:
        """Update button states based on job status."""
        if job_vm is None:
            self._start_btn.config(state=tk.NORMAL)
            self._pause_btn.config(state=tk.DISABLED)
            self._resume_btn.config(state=tk.DISABLED)
            self._cancel_btn.config(state=tk.DISABLED)
        else:
            self._start_btn.config(state=tk.DISABLED if job_vm.can_pause or job_vm.can_resume else tk.NORMAL)
            self._pause_btn.config(state=tk.NORMAL if job_vm.can_pause else tk.DISABLED)
            self._resume_btn.config(state=tk.NORMAL if job_vm.can_resume else tk.DISABLED)
            self._cancel_btn.config(state=tk.NORMAL if job_vm.can_cancel else tk.DISABLED)


class JobHistoryPanel(ttk.LabelFrame):
    """Panel displaying job history."""

    def __init__(self, parent: tk.Widget, **kwargs):
        super().__init__(parent, text="Job History", **kwargs)
        self._setup_ui()

    def _setup_ui(self):
        # Create treeview for job list
        columns = ('source', 'status', 'started', 'completed')
        self._tree = ttk.Treeview(self, columns=columns, show='headings', height=5)

        self._tree.heading('source', text='Source')
        self._tree.heading('status', text='Status')
        self._tree.heading('started', text='Started')
        self._tree.heading('completed', text='Completed')

        self._tree.column('source', width=200)
        self._tree.column('status', width=80)
        self._tree.column('started', width=150)
        self._tree.column('completed', width=150)

        self._tree.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Scrollbar
        scrollbar = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self._tree.yview)
        self._tree.configure(yscrollcommand=scrollbar.set)

    def add_job_to_history(self, job_vm: JobViewModel) -> None:
        """Add a completed job to the history list."""
        self._tree.insert('', 0, values=(
            job_vm.source_display,
            job_vm.status_display,
            job_vm.started_at_display,
            job_vm.completed_at_display
        ))

    def clear_history(self) -> None:
        """Clear all job history."""
        for item in self._tree.get_children():
            self._tree.delete(item)
