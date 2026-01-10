# Web GUI Package
"""
Web-based GUI using FastAPI.
Provides REST API and serves static HTML/JS frontend.
Works on headless servers (Hetzner VMs) via browser access.
"""
from .api import create_app, ScraperAPI

__all__ = ['create_app', 'ScraperAPI']
