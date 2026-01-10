#!/usr/bin/env python
"""
CAS Scraper Entry Point.

Main entry point for the CAS/TAS Jurisprudence Scraper CLI.

Usage:
    python -m src.cas_main discover --max-results 50
    python -m src.cas_main discover --year-from 2020 --sport football
    python -m src.cas_main status
    python -m src.cas_main resume --session-id abc123
    python -m src.cas_main list-checkpoints
    python -m src.cas_main config --show
"""
import sys


def main() -> int:
    """Main entry point for CAS CLI."""
    from src.infrastructure.cli.cas_cli import main as cli_main
    return cli_main()


if __name__ == "__main__":
    sys.exit(main())
