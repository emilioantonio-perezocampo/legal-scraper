#!/usr/bin/env python3
"""
BJV Scraper CLI - Biblioteca Jurídica Virtual UNAM

Usage:
    python -m src.bjv_main discover [options]
    python -m src.bjv_main scrape [options]
    python -m src.bjv_main status [options]
    python -m src.bjv_main resume [options]

Examples:
    # Discover books
    python -m src.bjv_main discover --max-results 50

    # Discover by area
    python -m src.bjv_main discover --area "Derecho Civil" --max-results 100

    # Full scrape with embeddings
    python -m src.bjv_main scrape --max-results 20 --output-dir ./bjv_data

    # Check status
    python -m src.bjv_main status --session-id abc123

    # Resume interrupted session
    python -m src.bjv_main resume --session-id abc123
"""
import argparse
import asyncio
import json
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Any, Dict

from src.infrastructure.bjv_session_manager import BJVSessionManager


# Exit codes
EXIT_SUCCESS = 0
EXIT_ERROR = 1
EXIT_INTERRUPTED = 2


def create_parser() -> argparse.ArgumentParser:
    """Create CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="bjv_scraper",
        description="BJV Scraper - Biblioteca Jurídica Virtual UNAM",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s discover --max-results 50
  %(prog)s discover --area "Derecho Civil" --year-from 2020
  %(prog)s scrape --max-results 20 --output-dir ./data
  %(prog)s status --session-id abc123
  %(prog)s resume --session-id abc123
        """,
    )

    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 1.0.0",
    )

    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON",
    )

    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        title="commands",
        description="Available commands",
    )

    # =========================================================================
    # discover command
    # =========================================================================
    discover_parser = subparsers.add_parser(
        "discover",
        help="Discover books from BJV search",
        description="Search BJV and list discovered books without downloading",
    )

    discover_parser.add_argument(
        "--query", "-q",
        type=str,
        help="Search query text",
    )

    discover_parser.add_argument(
        "--area",
        type=str,
        help="Legal area filter (e.g., 'Derecho Civil', 'Derecho Penal')",
    )

    discover_parser.add_argument(
        "--year-from",
        type=int,
        help="Filter books from this year",
    )

    discover_parser.add_argument(
        "--year-to",
        type=int,
        help="Filter books until this year",
    )

    discover_parser.add_argument(
        "--max-results",
        type=int,
        default=50,
        help="Maximum number of results (default: 50)",
    )

    # =========================================================================
    # scrape command
    # =========================================================================
    scrape_parser = subparsers.add_parser(
        "scrape",
        help="Full scrape with PDF download and embeddings",
        description="Discover, download PDFs, extract text, and generate embeddings",
    )

    scrape_parser.add_argument(
        "--query", "-q",
        type=str,
        help="Search query text",
    )

    scrape_parser.add_argument(
        "--area",
        type=str,
        help="Legal area filter",
    )

    scrape_parser.add_argument(
        "--year-from",
        type=int,
        help="Filter books from this year",
    )

    scrape_parser.add_argument(
        "--year-to",
        type=int,
        help="Filter books until this year",
    )

    scrape_parser.add_argument(
        "--max-results",
        type=int,
        default=20,
        help="Maximum number of books to process (default: 20)",
    )

    scrape_parser.add_argument(
        "--output-dir", "-o",
        type=str,
        default="bjv_data",
        help="Output directory for downloaded files (default: bjv_data)",
    )

    scrape_parser.add_argument(
        "--skip-embeddings",
        action="store_true",
        help="Skip embedding generation",
    )

    scrape_parser.add_argument(
        "--rate-limit",
        type=float,
        default=0.5,
        help="Requests per second (default: 0.5)",
    )

    scrape_parser.add_argument(
        "--concurrent",
        type=int,
        default=3,
        help="Maximum concurrent downloads (default: 3)",
    )

    # =========================================================================
    # status command
    # =========================================================================
    status_parser = subparsers.add_parser(
        "status",
        help="Check scraping session status",
        description="Display status of a scraping session",
    )

    status_parser.add_argument(
        "--session-id",
        type=str,
        help="Session ID to check (optional, shows latest if not specified)",
    )

    status_parser.add_argument(
        "--output-dir", "-o",
        type=str,
        default="bjv_data",
        help="Output directory containing session data",
    )

    # =========================================================================
    # resume command
    # =========================================================================
    resume_parser = subparsers.add_parser(
        "resume",
        help="Resume an interrupted session",
        description="Resume a previously interrupted scraping session from checkpoint",
    )

    resume_parser.add_argument(
        "--session-id",
        type=str,
        required=True,
        help="Session ID to resume",
    )

    resume_parser.add_argument(
        "--output-dir", "-o",
        type=str,
        default="bjv_data",
        help="Output directory containing session data",
    )

    return parser


class BJVCLIHandler:
    """Handler for BJV CLI commands."""

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.coordinator: Optional[Any] = None
        self._interrupted = False
        self._session_manager: Optional[BJVSessionManager] = None

    def setup_signal_handlers(self) -> None:
        """Setup graceful shutdown on SIGINT/SIGTERM."""
        def handler(signum, frame):
            self._interrupted = True
            if not self.args.json:
                print("\n  Interrumpido. Guardando checkpoint...")

        signal.signal(signal.SIGINT, handler)
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, handler)

    def _create_coordinator(self, **kwargs) -> Any:
        """Create coordinator actor (placeholder for future integration)."""
        from src.infrastructure.actors.bjv_coordinator_actor import BJVCoordinatorActor
        return BJVCoordinatorActor(**kwargs)

    async def run(self) -> int:
        """Run the appropriate command."""
        self.setup_signal_handlers()

        command = self.args.command

        if command == "discover":
            return await self.cmd_discover()
        elif command == "scrape":
            return await self.cmd_scrape()
        elif command == "status":
            return await self.cmd_status()
        elif command == "resume":
            return await self.cmd_resume()
        else:
            self._print_error("No command specified. Use --help for usage.")
            return EXIT_ERROR

    async def cmd_discover(self) -> int:
        """Execute discover command."""
        self._print_header("Descubriendo libros en BJV...")

        try:
            self.coordinator = self._create_coordinator()
            await self.coordinator.start()

            # Send discovery command
            from src.infrastructure.actors.bjv_messages import IniciarBusqueda

            msg = IniciarBusqueda(
                correlation_id="cli-discover",
                query=self.args.query,
                area_derecho=self.args.area,
                anio_desde=self.args.year_from,
                anio_hasta=self.args.year_to,
                max_resultados=self.args.max_results,
            )

            result = await self.coordinator.ask(msg, timeout=300.0)

            self._print_discover_results(result)
            return EXIT_SUCCESS

        except Exception as e:
            self._print_error(str(e))
            return EXIT_ERROR
        finally:
            if self.coordinator:
                await self.coordinator.stop()

    async def cmd_scrape(self) -> int:
        """Execute full scrape command."""
        self._print_header("Iniciando scraping completo de BJV...")

        output_dir = Path(self.args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        self._session_manager = BJVSessionManager(str(output_dir))

        try:
            self.coordinator = self._create_coordinator(
                download_dir=str(output_dir),
            )
            await self.coordinator.start()

            from src.infrastructure.actors.bjv_messages import (
                IniciarPipeline,
                DetenerPipeline,
                ObtenerEstado,
            )

            session_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

            msg = IniciarPipeline(
                session_id=session_id,
                max_resultados=self.args.max_results,
                query=self.args.query,
            )

            await self.coordinator.tell(msg)

            # Monitor progress until complete or interrupted
            estado = {}
            while not self._interrupted:
                estado = await self.coordinator.ask(ObtenerEstado(), timeout=10.0)

                self._print_progress(estado)

                state = estado.get("state", "")
                if state.lower() in ("completed", "error", "failed"):
                    break

                await asyncio.sleep(2.0)

            if self._interrupted:
                await self.coordinator.tell(DetenerPipeline(guardar_checkpoint=True))
                await asyncio.sleep(1.0)
                return EXIT_INTERRUPTED

            self._print_final_stats(estado)
            return EXIT_SUCCESS

        except Exception as e:
            self._print_error(str(e))
            return EXIT_ERROR
        finally:
            if self.coordinator:
                await self.coordinator.stop()

    async def cmd_status(self) -> int:
        """Execute status command."""
        output_dir = Path(self.args.output_dir)

        if not output_dir.exists():
            self._print_error(f"Output directory not found: {output_dir}")
            return EXIT_ERROR

        if self.args.session_id:
            # Looking for a specific session
            checkpoint_file = output_dir / f"checkpoint_{self.args.session_id}.json"
            if not checkpoint_file.exists():
                self._print_error(f"Session not found: {self.args.session_id}")
                return EXIT_ERROR
            checkpoints = [checkpoint_file]
        else:
            # Find checkpoint files
            checkpoints = list(output_dir.glob("checkpoint_*.json"))

        if not checkpoints:
            if self.args.json:
                print(json.dumps({"sessions": []}))
            else:
                print("No sessions found.")
            return EXIT_SUCCESS

        for cp_file in sorted(checkpoints, reverse=True)[:5]:
            self._print_checkpoint_status(cp_file)

        return EXIT_SUCCESS

    async def cmd_resume(self) -> int:
        """Execute resume command."""
        output_dir = Path(self.args.output_dir)
        checkpoint_file = output_dir / f"checkpoint_{self.args.session_id}.json"

        if not checkpoint_file.exists():
            self._print_error(f"Checkpoint not found: {checkpoint_file}")
            return EXIT_ERROR

        self._print_header(f"Reanudando sesión {self.args.session_id}...")

        try:
            self.coordinator = self._create_coordinator(
                download_dir=str(output_dir),
            )
            await self.coordinator.start()

            from src.infrastructure.actors.bjv_messages import (
                ReanudarPipeline,
                DetenerPipeline,
                ObtenerEstado,
            )

            msg = ReanudarPipeline(session_id=self.args.session_id)
            await self.coordinator.tell(msg)

            # Monitor progress
            estado = {}
            while not self._interrupted:
                estado = await self.coordinator.ask(ObtenerEstado(), timeout=10.0)
                self._print_progress(estado)

                state = estado.get("state", "")
                if state.lower() in ("completed", "error", "failed"):
                    break

                await asyncio.sleep(2.0)

            if self._interrupted:
                await self.coordinator.tell(DetenerPipeline(guardar_checkpoint=True))
                return EXIT_INTERRUPTED

            self._print_final_stats(estado)
            return EXIT_SUCCESS

        except Exception as e:
            self._print_error(str(e))
            return EXIT_ERROR
        finally:
            if self.coordinator:
                await self.coordinator.stop()

    # =========================================================================
    # Output helpers
    # =========================================================================

    def _print_header(self, text: str) -> None:
        if self.args.json:
            return
        print(f"\n{'='*60}")
        print(f"  {text}")
        print(f"{'='*60}\n")

    def _print_error(self, message: str) -> None:
        if self.args.json:
            print(json.dumps({"error": message}))
        else:
            print(f"Error: {message}")

    def _print_discover_results(self, result: Dict) -> None:
        if self.args.json:
            print(json.dumps(result, indent=2, default=str))
        else:
            total = result.get('total', 0)
            print(f"Libros descubiertos: {total}")
            for libro in result.get("libros", [])[:10]:
                titulo = libro.get('titulo', 'Sin título')[:60]
                print(f"  - {titulo}")
            if total > 10:
                print(f"  ... y {total - 10} más")

    def _print_progress(self, estado: Dict) -> None:
        if self.args.json:
            return
        stats = estado.get("stats", estado)
        descubiertos = stats.get('total_descubiertos', stats.get('descubiertos', 0))
        procesados = stats.get('total_procesados', stats.get('procesados', 0))
        errores = stats.get('total_errores', stats.get('errores', 0))
        print(
            f"\r  Descubiertos: {descubiertos} | "
            f"Procesados: {procesados} | "
            f"Errores: {errores}",
            end="",
            flush=True,
        )

    def _print_final_stats(self, estado: Dict) -> None:
        if self.args.json:
            print(json.dumps(estado, indent=2, default=str))
        else:
            stats = estado.get("stats", estado)
            print(f"\n\n{'='*60}")
            print("  Scraping completado")
            print(f"{'='*60}")
            print(f"  Libros descubiertos:  {stats.get('total_descubiertos', 0)}")
            print(f"  Libros procesados:    {stats.get('total_procesados', 0)}")
            print(f"  Errores:              {stats.get('total_errores', 0)}")
            print(f"{'='*60}\n")

    def _print_checkpoint_status(self, checkpoint_file: Path) -> None:
        with open(checkpoint_file) as f:
            data = json.load(f)

        if self.args.json:
            print(json.dumps(data, indent=2))
        else:
            print(f"\n  Session: {data.get('session_id', 'unknown')}")
            print(f"   Estado: {data.get('state', 'unknown')}")
            print(f"   Último update: {data.get('updated_at', 'unknown')}")
            stats = data.get("stats", {})
            procesados = stats.get('procesados', stats.get('total_procesados', 0))
            descubiertos = stats.get('descubiertos', stats.get('total_descubiertos', 0))
            print(f"   Progreso: {procesados}/{descubiertos}")


def main() -> int:
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return EXIT_ERROR

    handler = BJVCLIHandler(args)
    return asyncio.run(handler.run())


if __name__ == "__main__":
    sys.exit(main())
