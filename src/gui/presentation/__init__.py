# GUI Presentation Layer
"""
Presentation layer containing:
- View Models: UI-optimized representations of domain entities
- Presenters: Logic for updating views based on state changes
- Views: UI components (Tkinter-based)
"""
from .view_models import (
    JobViewModel,
    ProgressViewModel,
    LogEntryViewModel,
    ConfigurationViewModel,
)
from .presenters import (
    MainPresenter,
    JobListPresenter,
)

__all__ = [
    'JobViewModel',
    'ProgressViewModel',
    'LogEntryViewModel',
    'ConfigurationViewModel',
    'MainPresenter',
    'JobListPresenter',
]
