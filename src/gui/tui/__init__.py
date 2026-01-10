# Terminal TUI Package
"""
Terminal User Interface using Textual.
Works over SSH on headless servers (Hetzner VMs).
"""
from .app import ScraperTUI, StatusWidget, ProgressWidget, LogWidget

__all__ = ['ScraperTUI', 'StatusWidget', 'ProgressWidget', 'LogWidget']
