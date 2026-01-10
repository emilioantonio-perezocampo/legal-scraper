#!/usr/bin/env python3
"""
Legal Scraper GUI Launcher

Unified launcher script that provides access to all GUI modes:
- web: Web-based GUI (FastAPI) - for remote access via browser
- tui: Terminal TUI (Textual) - for SSH access on headless servers
- desktop: Desktop GUI (Tkinter) - for local machines with display

Usage:
    python -m src.gui.launcher --mode web --port 8000
    python -m src.gui.launcher --mode tui
    python -m src.gui.launcher --mode desktop
    python -m src.gui.launcher --help
"""
import argparse
import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


def run_web_gui(host: str = "0.0.0.0", port: int = 8000):
    """Run the web-based GUI (FastAPI)."""
    print(f"Starting Web GUI on http://{host}:{port}")
    print("Access the control panel from any browser on your network.")
    print("Press Ctrl+C to stop.\n")

    try:
        import uvicorn
        from src.gui.web.api import create_app

        app = create_app()
        uvicorn.run(app, host=host, port=port)
    except ImportError as e:
        print(f"Error: Missing dependencies for web GUI: {e}")
        print("Install with: pip install fastapi uvicorn")
        sys.exit(1)


def run_tui_gui():
    """Run the terminal TUI (Textual)."""
    print("Starting Terminal TUI...")
    print("Use keyboard shortcuts: s=Start, p=Pause, r=Resume, c=Cancel, q=Quit\n")

    try:
        from src.gui.tui.app import ScraperTUI

        app = ScraperTUI()
        app.run()
    except ImportError as e:
        print(f"Error: Missing dependencies for TUI: {e}")
        print("Install with: pip install textual rich")
        sys.exit(1)


def run_desktop_gui():
    """Run the desktop GUI (Tkinter)."""
    print("Starting Desktop GUI...")

    # Check if display is available
    if sys.platform != 'win32':
        display = os.environ.get('DISPLAY')
        if not display:
            print("Error: No display available.")
            print("For headless servers, use --mode web or --mode tui instead.")
            sys.exit(1)

    try:
        from src.gui.main import GuiApplication

        app = GuiApplication()
        app.run()
    except ImportError as e:
        print(f"Error: Missing dependencies for desktop GUI: {e}")
        print("Tkinter is usually included with Python.")
        sys.exit(1)
    except Exception as e:
        if "display" in str(e).lower() or "cannot open" in str(e).lower():
            print(f"Error: Cannot open display: {e}")
            print("For headless servers, use --mode web or --mode tui instead.")
            sys.exit(1)
        raise


def main():
    """Main entry point for the launcher."""
    parser = argparse.ArgumentParser(
        description="Legal Scraper GUI Launcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
GUI Modes:
  web      Web-based GUI accessible via browser (best for remote servers)
  tui      Terminal TUI that works over SSH (best for headless servers)
  desktop  Desktop GUI with Tkinter (requires display)

Examples:
  %(prog)s --mode web --port 8080     # Start web GUI on port 8080
  %(prog)s --mode tui                 # Start terminal TUI
  %(prog)s --mode desktop             # Start desktop GUI (requires display)
  %(prog)s -m web -H 0.0.0.0          # Web GUI accessible from network
"""
    )

    parser.add_argument(
        "-m", "--mode",
        choices=["web", "tui", "desktop"],
        default="web",
        help="GUI mode to run (default: web)"
    )

    parser.add_argument(
        "-H", "--host",
        default="0.0.0.0",
        help="Host to bind web server (default: 0.0.0.0)"
    )

    parser.add_argument(
        "-p", "--port",
        type=int,
        default=8000,
        help="Port for web server (default: 8000)"
    )

    parser.add_argument(
        "--version",
        action="version",
        version="Legal Scraper GUI 1.0.0"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("  Legal Scraper - DOF Document Collector")
    print("=" * 60)
    print()

    if args.mode == "web":
        run_web_gui(host=args.host, port=args.port)
    elif args.mode == "tui":
        run_tui_gui()
    elif args.mode == "desktop":
        run_desktop_gui()


if __name__ == "__main__":
    main()
