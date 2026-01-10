"""
CAS Scraper Command-Line Interface.

Provides commands for:
- discover: Search and scrape CAS awards
- status: Check pipeline status
- resume: Resume from checkpoint
- list-checkpoints: List available checkpoints
- config: Show/set configuration
"""
import argparse
import asyncio
import sys
import logging
from typing import Optional, List
from datetime import datetime
from pathlib import Path

from src.infrastructure.cli.cas_config import (
    CASCliConfig,
    SearchFilters,
    AVAILABLE_SPORTS,
    AVAILABLE_MATTERS,
    AVAILABLE_PROCEDURES,
)
from src.domain.cas_value_objects import CategoriaDeporte, TipoMateria


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser for CAS CLI."""
    parser = argparse.ArgumentParser(
        prog="cas-scraper",
        description="CAS/TAS Jurisprudence Scraper - Download and process arbitration awards",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s discover --max-results 50
  %(prog)s discover --year-from 2020 --year-to 2024 --sport football
  %(prog)s discover --matter doping --max-results 100
  %(prog)s status
  %(prog)s resume --session-id abc123
  %(prog)s list-checkpoints
        """,
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )

    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Suppress non-essential output",
    )

    parser.add_argument(
        "--config",
        type=str,
        help="Path to configuration file",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Discover command
    discover_parser = subparsers.add_parser(
        "discover",
        help="Search and scrape CAS awards",
        description="Search the CAS jurisprudence database and download awards",
    )
    _add_discover_arguments(discover_parser)

    # Status command
    status_parser = subparsers.add_parser(
        "status",
        help="Check current pipeline status",
    )
    status_parser.add_argument(
        "--session-id",
        type=str,
        help="Check status of specific session",
    )

    # Resume command
    resume_parser = subparsers.add_parser(
        "resume",
        help="Resume scraping from checkpoint",
    )
    resume_parser.add_argument(
        "--session-id",
        type=str,
        required=True,
        help="Session ID to resume",
    )

    # List checkpoints command
    list_parser = subparsers.add_parser(
        "list-checkpoints",
        help="List available checkpoints",
    )
    list_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum checkpoints to show (default: 10)",
    )

    # Config command
    config_parser = subparsers.add_parser(
        "config",
        help="Show or set configuration",
    )
    config_parser.add_argument(
        "--show",
        action="store_true",
        help="Show current configuration",
    )

    return parser


def _add_discover_arguments(parser: argparse.ArgumentParser) -> None:
    """Add arguments for the discover command."""
    # Filter arguments
    parser.add_argument(
        "--year-from",
        type=int,
        help="Start year for search (e.g., 2020)",
    )

    parser.add_argument(
        "--year-to",
        type=int,
        help="End year for search (e.g., 2024)",
    )

    parser.add_argument(
        "--sport",
        type=str,
        choices=AVAILABLE_SPORTS,
        help=f"Filter by sport: {', '.join(AVAILABLE_SPORTS)}",
    )

    parser.add_argument(
        "--matter",
        type=str,
        choices=AVAILABLE_MATTERS,
        help=f"Filter by subject matter: {', '.join(AVAILABLE_MATTERS)}",
    )

    parser.add_argument(
        "--keyword",
        type=str,
        help="Search keyword",
    )

    parser.add_argument(
        "--procedure",
        type=str,
        choices=AVAILABLE_PROCEDURES,
        help=f"Filter by procedure type: {', '.join(AVAILABLE_PROCEDURES)}",
    )

    # Limits
    parser.add_argument(
        "--max-results",
        type=int,
        default=100,
        help="Maximum awards to download (default: 100)",
    )

    # Output options
    parser.add_argument(
        "--output-dir",
        type=str,
        default="cas_data",
        help="Output directory for downloaded data (default: cas_data)",
    )

    # Browser options
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Run browser in visible mode (for debugging)",
    )

    # Dry run
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without executing",
    )


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = create_parser()
    return parser.parse_args(args)


def setup_logging(
    verbose: bool = False,
    quiet: bool = False,
    log_dir: str = "cas_logs",
) -> logging.Logger:
    """Configure logging for CLI."""
    log_level = logging.DEBUG if verbose else (logging.WARNING if quiet else logging.INFO)

    # Create log directory
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    # Configure root logger
    logger = logging.getLogger("cas_scraper")
    logger.setLevel(log_level)

    # Clear existing handlers
    logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_format = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler
    log_file = Path(log_dir) / f"cas_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)

    return logger


def _map_sport_to_categoria(sport: str) -> Optional[CategoriaDeporte]:
    """Map CLI sport string to domain enum."""
    mapping = {
        "football": CategoriaDeporte.FUTBOL,
        "athletics": CategoriaDeporte.ATLETISMO,
        "cycling": CategoriaDeporte.CICLISMO,
        "swimming": CategoriaDeporte.NATACION,
        "basketball": CategoriaDeporte.BALONCESTO,
        "tennis": CategoriaDeporte.TENIS,
        "skiing": CategoriaDeporte.ESQUI,
        "other": CategoriaDeporte.OTRO,
    }
    return mapping.get(sport)


def _map_matter_to_tipo(matter: str) -> Optional[TipoMateria]:
    """Map CLI matter string to domain enum."""
    mapping = {
        "doping": TipoMateria.DOPAJE,
        "transfer": TipoMateria.TRANSFERENCIA,
        "eligibility": TipoMateria.ELEGIBILIDAD,
        "disciplinary": TipoMateria.DISCIPLINA,
        "contractual": TipoMateria.CONTRACTUAL,
        "governance": TipoMateria.GOBERNANZA,
        "other": TipoMateria.OTRO,
    }
    return mapping.get(matter)


async def run_discover(
    args: argparse.Namespace,
    config: CASCliConfig,
    logger: logging.Logger,
) -> int:
    """Execute the discover command."""
    # Build search filters
    filters = SearchFilters(
        year_from=args.year_from,
        year_to=args.year_to,
        sport=args.sport,
        matter=args.matter,
        keyword=getattr(args, 'keyword', None),
        procedure_type=getattr(args, 'procedure', None),
        max_results=args.max_results,
    )

    logger.info(f"Iniciando búsqueda CAS: {filters.describe()}")

    if args.dry_run:
        logger.info("Modo dry-run: mostrando configuración sin ejecutar")
        logger.info(f"  Filtros: {filters.to_dict()}")
        logger.info(f"  Output: {args.output_dir}")
        logger.info(f"  Headless: {not args.no_headless}")
        return 0

    # Ensure output directories exist
    config.ensure_directories()

    # TODO: Create coordinator and run pipeline
    # For now, just log that we would start
    logger.info("Pipeline execution not yet implemented")
    logger.info("Use --dry-run to see configuration")

    return 0


async def run_status(
    args: argparse.Namespace,
    config: CASCliConfig,
    logger: logging.Logger,
) -> int:
    """Execute the status command."""
    logger.info("Estado del scraper CAS:")

    # Check for active sessions (would read from checkpoint files)
    checkpoint_dir = Path(config.checkpoint_dir)

    if not checkpoint_dir.exists():
        logger.info("  No hay sesiones activas")
        return 0

    checkpoints = list(checkpoint_dir.glob("*.json"))

    if not checkpoints:
        logger.info("  No hay checkpoints guardados")
        return 0

    logger.info(f"  {len(checkpoints)} checkpoint(s) encontrado(s)")

    for cp in checkpoints[:5]:
        logger.info(f"    - {cp.stem}")

    return 0


async def run_resume(
    args: argparse.Namespace,
    config: CASCliConfig,
    logger: logging.Logger,
) -> int:
    """Execute the resume command."""
    session_id = args.session_id
    logger.info(f"Reanudando sesión: {session_id}")

    checkpoint_file = Path(config.checkpoint_dir) / f"{session_id}.json"

    if not checkpoint_file.exists():
        logger.error(f"Checkpoint no encontrado: {session_id}")
        return 1

    # Load checkpoint and resume (simplified)
    logger.info("Cargando checkpoint...")

    # Would create coordinator with checkpoint data
    # For now, just indicate success
    logger.info("Funcionalidad de resume pendiente de implementación completa")

    return 0


async def run_list_checkpoints(
    args: argparse.Namespace,
    config: CASCliConfig,
    logger: logging.Logger,
) -> int:
    """Execute the list-checkpoints command."""
    checkpoint_dir = Path(config.checkpoint_dir)

    if not checkpoint_dir.exists():
        logger.info("No hay directorio de checkpoints")
        return 0

    checkpoints = sorted(checkpoint_dir.glob("*.json"), reverse=True)

    if not checkpoints:
        logger.info("No hay checkpoints guardados")
        return 0

    logger.info(f"Checkpoints disponibles ({len(checkpoints)} total):")

    for cp in checkpoints[:args.limit]:
        # Would parse checkpoint for more details
        mtime = datetime.fromtimestamp(cp.stat().st_mtime)
        logger.info(f"  {cp.stem} - {mtime.strftime('%Y-%m-%d %H:%M:%S')}")

    if len(checkpoints) > args.limit:
        logger.info(f"  ... y {len(checkpoints) - args.limit} más")

    return 0


async def run_config(
    args: argparse.Namespace,
    config: CASCliConfig,
    logger: logging.Logger,
) -> int:
    """Execute the config command."""
    if args.show:
        logger.info("Configuración actual:")
        for key, value in config.to_dict().items():
            logger.info(f"  {key}: {value}")
    else:
        logger.info("Usa --show para ver la configuración actual")

    return 0


async def main_async(args: Optional[List[str]] = None) -> int:
    """Async main entry point."""
    parsed_args = parse_args(args)

    if not parsed_args.command:
        create_parser().print_help()
        return 0

    # Load configuration
    config = CASCliConfig.from_env()

    # Setup logging
    logger = setup_logging(
        verbose=parsed_args.verbose,
        quiet=parsed_args.quiet,
        log_dir=config.log_dir,
    )

    # Dispatch to command handler
    command_handlers = {
        "discover": run_discover,
        "status": run_status,
        "resume": run_resume,
        "list-checkpoints": run_list_checkpoints,
        "config": run_config,
    }

    handler = command_handlers.get(parsed_args.command)

    if handler:
        return await handler(parsed_args, config, logger)
    else:
        logger.error(f"Comando desconocido: {parsed_args.command}")
        return 1


def main(args: Optional[List[str]] = None) -> int:
    """Synchronous main entry point."""
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
