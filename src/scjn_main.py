#!/usr/bin/env python3
"""
SCJN Legislation Scraper - Main Entry Point

Usage:
    python -m src.scjn_main discover --max-results 100
    python -m src.scjn_main discover --category LEY --scope FEDERAL
    python -m src.scjn_main status
    python -m src.scjn_main resume --session-id <id>
"""
import argparse
import asyncio
import sys
from pathlib import Path

from src.infrastructure.actors.scjn_coordinator_actor import (
    SCJNCoordinatorActor,
    PipelineState,
)
from src.infrastructure.actors.scjn_discovery_actor import SCJNDiscoveryActor
from src.infrastructure.actors.scjn_scraper_actor import SCJNScraperActor
from src.infrastructure.actors.persistence_actor import SCJNPersistenceActor
from src.infrastructure.actors.checkpoint_actor import CheckpointActor
from src.infrastructure.actors.rate_limiter import RateLimiter
from src.infrastructure.actors.messages import (
    DescubrirDocumentos,
    ObtenerEstado,
    PausarPipeline,
    CargarCheckpoint,
)


async def create_pipeline(args) -> SCJNCoordinatorActor:
    """Create and wire the full scraping pipeline."""
    # Create rate limiter
    rate_limiter = RateLimiter(
        requests_per_second=args.rate_limit,
    )

    # Create child actors
    persistence = SCJNPersistenceActor(storage_dir=args.output_dir)
    checkpoint = CheckpointActor(checkpoint_dir=args.checkpoint_dir)

    # Create coordinator (it will create discovery/scraper internally)
    coordinator = SCJNCoordinatorActor(
        discovery_actor=None,  # Will be wired
        scraper_actor=None,
        persistence_actor=persistence,
        checkpoint_actor=checkpoint,
        rate_limiter=rate_limiter,
        max_concurrent_downloads=args.concurrency,
    )

    # Wire discovery and scraper actors
    discovery = SCJNDiscoveryActor(
        coordinator=coordinator,
        rate_limiter=rate_limiter,
    )
    scraper = SCJNScraperActor(
        coordinator=coordinator,
        rate_limiter=rate_limiter,
        download_pdfs=not args.skip_pdfs if hasattr(args, 'skip_pdfs') else True,
    )

    coordinator._discovery_actor = discovery
    coordinator._scraper_actor = scraper

    # Start all actors
    await persistence.start()
    await checkpoint.start()
    await discovery.start()
    await scraper.start()
    await coordinator.start()

    return coordinator


async def stop_pipeline(coordinator: SCJNCoordinatorActor):
    """Stop all pipeline actors cleanly."""
    if coordinator._discovery_actor:
        await coordinator._discovery_actor.stop()
    if coordinator._scraper_actor:
        await coordinator._scraper_actor.stop()
    if coordinator._persistence_actor:
        await coordinator._persistence_actor.stop()
    if coordinator._checkpoint_actor:
        await coordinator._checkpoint_actor.stop()
    await coordinator.stop()


async def run_discovery(args):
    """Run document discovery."""
    print("=" * 60)
    print("SCJN Legislation Scraper")
    print("=" * 60)
    print(f"Output directory: {args.output_dir}")
    print(f"Max results: {args.max_results}")
    print(f"Rate limit: {args.rate_limit} req/sec")
    print(f"Concurrency: {args.concurrency}")
    if args.category:
        print(f"Category filter: {args.category}")
    if args.scope:
        print(f"Scope filter: {args.scope}")
    if args.status:
        print(f"Status filter: {args.status}")
    print("=" * 60)
    print()

    # Ensure directories exist
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    Path(args.checkpoint_dir).mkdir(parents=True, exist_ok=True)

    coordinator = await create_pipeline(args)

    try:
        print("[OK] All actors initialized.")
        print()

        # Build discovery command
        cmd = DescubrirDocumentos(
            category=args.category,
            scope=args.scope,
            status=args.status,
            max_results=args.max_results,
            discover_all_pages=args.all_pages if hasattr(args, 'all_pages') else False,
        )

        print(f"[SEARCH] Starting discovery (max {args.max_results} documents)...")
        print()

        # Start discovery
        result = await coordinator.ask(cmd)

        if hasattr(result, 'documents_found'):
            print(f"[INFO] Initial page: {result.documents_found} documents found")
            print(f"[INFO] Total pages: {result.total_pages}")
            print()

        # Progress reporting loop
        last_downloaded = 0
        stall_count = 0
        max_stalls = 10  # Exit after 10 consecutive stalls

        while True:
            await asyncio.sleep(3)

            status = await coordinator.ask(ObtenerEstado())

            discovered = status['discovered_count']
            downloaded = status['downloaded_count']
            pending = status['pending_count']
            errors = status['error_count']
            active = status['active_downloads']

            print(f"[PROGRESS] Discovered={discovered} | "
                  f"Downloaded={downloaded} | "
                  f"Pending={pending} | "
                  f"Active={active} | "
                  f"Errors={errors}")

            # Check for stall
            if downloaded == last_downloaded:
                stall_count += 1
            else:
                stall_count = 0
                last_downloaded = downloaded

            # Check if done
            if (pending == 0 and active == 0 and discovered > 0) or stall_count >= max_stalls:
                print()
                if stall_count >= max_stalls:
                    print("[WARN] Processing appears stalled, exiting.")
                else:
                    print("[OK] Discovery complete!")
                print(f"[SUMMARY] Total discovered: {discovered}")
                print(f"[SUMMARY] Total downloaded: {downloaded}")
                print(f"[SUMMARY] Total errors: {errors}")
                break

    except KeyboardInterrupt:
        print()
        print("[PAUSE] Interrupted - saving checkpoint...")
        await coordinator.ask(PausarPipeline())
    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        await stop_pipeline(coordinator)
        print("[OK] Shutdown complete.")


async def show_status(args):
    """Show checkpoint status."""
    checkpoint_dir = Path(args.checkpoint_dir)

    if not checkpoint_dir.exists():
        print("No checkpoint directory found.")
        return

    checkpoint_files = list(checkpoint_dir.glob("*.json"))

    if not checkpoint_files:
        print("No checkpoints found.")
        return

    print("Available checkpoints:")
    print("-" * 40)

    for cp_file in checkpoint_files:
        session_id = cp_file.stem
        print(f"  - {session_id}")

    print("-" * 40)
    print(f"Total: {len(checkpoint_files)} checkpoint(s)")


async def resume_session(args):
    """Resume from checkpoint."""
    print(f"[RESUME] Loading session: {args.session_id}")

    checkpoint_file = Path(args.checkpoint_dir) / f"{args.session_id}.json"

    if not checkpoint_file.exists():
        print(f"[ERROR] Checkpoint not found: {args.session_id}")
        return

    # For now, just show checkpoint info
    # Full resume would need to reload pending q_params
    import json
    with open(checkpoint_file) as f:
        data = json.load(f)

    print(f"[INFO] Session: {data.get('session_id', 'unknown')}")
    print(f"[INFO] Processed: {data.get('processed_count', 0)} documents")
    print(f"[INFO] Last processed: {data.get('last_processed_q_param', 'none')}")

    if data.get('failed_q_params'):
        print(f"[INFO] Failed: {len(data['failed_q_params'])} documents")

    print()
    print("[NOTE] Resume functionality would continue from this checkpoint.")
    print("[NOTE] Run 'discover' to start a new session.")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="SCJN Legislation Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.scjn_main discover --max-results 10
  python -m src.scjn_main discover --category LEY --scope FEDERAL
  python -m src.scjn_main status
  python -m src.scjn_main resume --session-id abc123
        """,
    )

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Discover command
    discover_parser = subparsers.add_parser('discover', help='Discover and scrape documents')
    discover_parser.add_argument(
        '--max-results', type=int, default=100,
        help='Maximum documents to discover (default: 100)',
    )
    discover_parser.add_argument(
        '--category', type=str, default=None,
        help='Filter by category (LEY, CODIGO, REGLAMENTO, etc.)',
    )
    discover_parser.add_argument(
        '--scope', type=str, default=None,
        help='Filter by scope (FEDERAL, ESTATAL, etc.)',
    )
    discover_parser.add_argument(
        '--status', type=str, default=None,
        help='Filter by status (VIGENTE, ABROGADA, etc.)',
    )
    discover_parser.add_argument(
        '--output-dir', type=str, default='scjn_data',
        help='Output directory (default: scjn_data)',
    )
    discover_parser.add_argument(
        '--checkpoint-dir', type=str, default='checkpoints',
        help='Checkpoint directory (default: checkpoints)',
    )
    discover_parser.add_argument(
        '--concurrency', type=int, default=3,
        help='Max concurrent downloads (default: 3)',
    )
    discover_parser.add_argument(
        '--rate-limit', type=float, default=0.5,
        help='Requests per second (default: 0.5)',
    )
    discover_parser.add_argument(
        '--skip-pdfs', action='store_true',
        help='Skip PDF downloads',
    )
    discover_parser.add_argument(
        '--all-pages', action='store_true',
        help='Discover all pages (not just first)',
    )

    # Status command
    status_parser = subparsers.add_parser('status', help='Show checkpoint status')
    status_parser.add_argument(
        '--checkpoint-dir', type=str, default='checkpoints',
        help='Checkpoint directory (default: checkpoints)',
    )

    # Resume command
    resume_parser = subparsers.add_parser('resume', help='Resume from checkpoint')
    resume_parser.add_argument(
        '--session-id', type=str, required=True,
        help='Session ID to resume',
    )
    resume_parser.add_argument(
        '--output-dir', type=str, default='scjn_data',
        help='Output directory (default: scjn_data)',
    )
    resume_parser.add_argument(
        '--checkpoint-dir', type=str, default='checkpoints',
        help='Checkpoint directory (default: checkpoints)',
    )

    args = parser.parse_args()

    if args.command == 'discover':
        asyncio.run(run_discovery(args))
    elif args.command == 'status':
        asyncio.run(show_status(args))
    elif args.command == 'resume':
        asyncio.run(resume_session(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
