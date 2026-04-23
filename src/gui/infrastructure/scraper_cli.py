#!/usr/bin/env python3
"""
CLI for Scraper Pipeline Management.

Commands:
    run <source>          - Run a single scraper pipeline immediately
    schedule list         - List all schedules
    schedule create       - Create a new schedule
    schedule delete <id>  - Delete a schedule
    status <workflow_id>  - Get workflow status

Usage:
    python -m src.gui.infrastructure.scraper_cli run scjn --max-results 10
    python -m src.gui.infrastructure.scraper_cli schedule create scjn-daily scjn "0 6 * * *"
    python -m src.gui.infrastructure.scraper_cli schedule list
"""
import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from datetime import timedelta

from temporalio.client import Client

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from src.gui.infrastructure.scraper_pipeline import (
    ScraperPipelineWorkflow,
    create_scraper_schedule,
    list_scraper_schedules,
    delete_scraper_schedule,
)
from src.gui.infrastructure.crawl4ai_workflow import (
    Crawl4AIExtractionWorkflow,
    create_crawl4ai_schedule,
)
from src.gui.infrastructure.mounted_backfill_workflow import (
    MountedBackfillDrainWorkflow,
)
from src.gui.infrastructure.mounted_disk_backfill import (
    SUPPORTED_BACKFILL_SOURCES,
    backfill_source_from_mounted_disk,
    backfill_source_from_mounted_disk_until_exhausted,
)
from src.gui.infrastructure.cas_chunk_benchmark import (
    DEFAULT_CORE_CANDIDATES,
    OPTIONAL_CANDIDATES,
    run_benchmark,
)
from src.gui.infrastructure.biblio_chunk_benchmark import (
    DEFAULT_CLASSIFIERS as BIBLIO_DEFAULT_CLASSIFIERS,
    DEFAULT_CORE_CANDIDATES as BIBLIO_DEFAULT_CORE_CANDIDATES,
    OPTIONAL_CANDIDATES as BIBLIO_OPTIONAL_CANDIDATES,
    OPTIONAL_CLASSIFIERS as BIBLIO_OPTIONAL_CLASSIFIERS,
    run_benchmark as run_biblio_benchmark,
)


async def get_client() -> Client:
    """Get Temporal client."""
    address = os.environ.get("TEMPORAL_ADDRESS", "temporal-temporal-1:7233")
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")
    return await Client.connect(address, namespace=namespace)


async def cmd_run(args):
    """Run a scraper pipeline immediately."""
    client = await get_client()
    task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "scraper-pipeline")

    config = {
        "source": args.source,
        "max_results": args.max_results,
    }

    # Add download options
    if getattr(args, 'download_pdfs', False):
        config["download_pdfs"] = True
    if getattr(args, 'upload_to_storage', False):
        config["upload_to_storage"] = True

    # Add source-specific options
    if args.source == "scjn":
        if args.category:
            config["category"] = args.category
        if args.scope:
            config["scope"] = args.scope
    elif args.source == "bjv":
        if args.search_term:
            config["search_term"] = args.search_term
        if args.area:
            config["area"] = args.area
    elif args.source == "cas":
        if args.sport:
            config["sport"] = args.sport
    elif args.source == "dof":
        config["mode"] = args.mode or "today"

    # Determine which workflow to use
    use_crawl4ai = getattr(args, 'use_crawl4ai', False)
    workflow_type = "crawl4ai" if use_crawl4ai else "api"
    workflow_id = f"{args.source}-{workflow_type}-{asyncio.get_event_loop().time():.0f}"

    print(f"Starting {args.source} pipeline ({workflow_type} mode)...")
    print(f"  Workflow ID: {workflow_id}")
    print(f"  Config: {config}")

    if use_crawl4ai:
        # Use Crawl4AI direct extraction workflow
        handle = await client.start_workflow(
            Crawl4AIExtractionWorkflow.run,
            args=[config],
            id=workflow_id,
            task_queue=task_queue,
        )
    else:
        # Use original API-based workflow
        handle = await client.start_workflow(
            ScraperPipelineWorkflow.run,
            args=[config],
            id=workflow_id,
            task_queue=task_queue,
        )

    print(f"  Started! Run ID: {handle.result_run_id}")

    if args.wait:
        print("  Waiting for completion...")
        result = await handle.result()
        print(f"  Result: {result}")
    else:
        print("  Use --wait to wait for completion")
        print(f"  Or check status: python -m src.gui.infrastructure.scraper_cli status {workflow_id}")


async def cmd_schedule_list(args):
    """List all scraper schedules."""
    client = await get_client()
    schedules = await list_scraper_schedules(client)

    if not schedules:
        print("No schedules found.")
        return

    print(f"Found {len(schedules)} schedule(s):\n")
    for s in schedules:
        print(f"  ID: {s['id']}")
        if s.get('info'):
            print(f"      Next run: {s['info'].next_action_times}")
        print()


async def cmd_schedule_create(args):
    """Create a scraper schedule."""
    client = await get_client()

    config = {}
    if args.max_results:
        config["max_results"] = args.max_results

    use_crawl4ai = getattr(args, 'use_crawl4ai', False)

    if use_crawl4ai:
        schedule_id = await create_crawl4ai_schedule(
            client=client,
            schedule_id=args.schedule_id,
            source=args.source,
            cron=args.cron,
            config=config if config else None,
        )
        workflow_type = "Crawl4AI"
    else:
        schedule_id = await create_scraper_schedule(
            client=client,
            schedule_id=args.schedule_id,
            source=args.source,
            cron=args.cron,
            config=config if config else None,
        )
        workflow_type = "API-based"

    print(f"Created schedule: {schedule_id}")
    print(f"  Source: {args.source}")
    print(f"  Cron: {args.cron}")
    print(f"  Workflow: {workflow_type}")


async def cmd_schedule_delete(args):
    """Delete a scraper schedule."""
    client = await get_client()
    success = await delete_scraper_schedule(client, args.schedule_id)

    if success:
        print(f"Deleted schedule: {args.schedule_id}")
    else:
        print(f"Failed to delete schedule: {args.schedule_id}")
        sys.exit(1)


async def cmd_status(args):
    """Get workflow status."""
    client = await get_client()

    try:
        handle = client.get_workflow_handle(args.workflow_id)
        desc = await handle.describe()

        print(f"Workflow: {args.workflow_id}")
        print(f"  Status: {desc.status.name}")
        print(f"  Start time: {desc.start_time}")
        if desc.close_time:
            print(f"  Close time: {desc.close_time}")

        # Try to get result if completed
        if desc.status.name == "COMPLETED":
            try:
                result = await handle.result()
                print(f"  Result: {result}")
            except Exception:
                pass

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


async def cmd_backfill_mounted(args):
    """Backfill mounted-disk source files into storage and queue indexing."""
    sources = list(SUPPORTED_BACKFILL_SOURCES) if args.source == "all" else [args.source]
    results = []
    client = await get_client() if args.via_temporal else None
    task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "scraper-pipeline") if args.via_temporal else None

    for source in sources:
        print(f"Backfilling mounted data for {source}...")
        if args.via_temporal:
            workflow_id = f"mounted-backfill-drain-{source}"
            workflow_config = {
                "source": source,
                "limit": args.limit,
                "include_existing_storage": not args.only_missing_storage,
                "bootstrap_missing_documents": not args.no_bootstrap_registration,
                "trigger_embedding": not args.no_trigger_embedding,
                "data_root": args.data_root,
                "max_passes": args.max_passes,
                "sleep_between_passes": args.sleep_between_passes,
            }
            try:
                handle = await client.start_workflow(
                    MountedBackfillDrainWorkflow.run,
                    args=[workflow_config],
                    id=workflow_id,
                    task_queue=task_queue,
                    task_timeout=timedelta(minutes=2),
                )
                started = {
                    "workflow_id": workflow_id,
                    "run_id": handle.result_run_id,
                    "source": source,
                    "status": "started",
                }
            except Exception as exc:
                if "already started" not in str(exc).lower():
                    raise
                handle = client.get_workflow_handle(workflow_id)
                started = {
                    "workflow_id": workflow_id,
                    "source": source,
                    "status": "already_running",
                }

            if args.wait:
                started["result"] = await handle.result()
            results.append(started)
            print(json.dumps(started, ensure_ascii=False, indent=2))
            if args.sleep_between_sources and source != sources[-1]:
                await asyncio.sleep(args.sleep_between_sources)
            continue

        if args.until_exhausted:
            result = await backfill_source_from_mounted_disk_until_exhausted(
                source=source,
                limit=args.limit,
                include_existing_storage=not args.only_missing_storage,
                bootstrap_missing_documents=not args.no_bootstrap_registration,
                trigger_embedding=not args.no_trigger_embedding,
                wait_for_workflow=args.wait,
                data_root=args.data_root,
                max_passes=args.max_passes,
                sleep_between_passes=args.sleep_between_passes,
            )
        else:
            result = await backfill_source_from_mounted_disk(
                source=source,
                limit=args.limit,
                include_existing_storage=not args.only_missing_storage,
                bootstrap_missing_documents=not args.no_bootstrap_registration,
                trigger_embedding=not args.no_trigger_embedding,
                wait_for_workflow=args.wait,
                data_root=args.data_root,
            )
        results.append(result.to_dict())
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))

        if args.sleep_between_sources and source != sources[-1]:
            await asyncio.sleep(args.sleep_between_sources)


async def cmd_benchmark_cas(args):
    """Run the CAS/TAS chunking benchmark harness."""
    output_dir = Path(os.path.abspath(args.output_dir))
    candidate_ids = [candidate.strip() for candidate in args.candidates.split(",") if candidate.strip()] if args.candidates else None
    doc_ids = [doc_id.strip() for doc_id in args.doc_ids.split(",") if doc_id.strip()] if args.doc_ids else None
    results = run_benchmark(
        output_dir=output_dir,
        data_root=Path(args.data_root),
        smoke_size=args.smoke_size,
        tuning_size=args.tuning_size,
        holdout_size=args.holdout_size,
        seed=args.seed,
        candidate_ids=candidate_ids,
        doc_ids=doc_ids,
        mode=args.mode,
        candidate_set=args.candidate_set,
        difficult_subset_size=args.difficult_subset_size,
        include_optional=args.include_optional,
        include_pdf_info=not args.skip_pdf_info,
    )
    print(json.dumps(results, ensure_ascii=False, indent=2))


async def cmd_benchmark_biblio(args):
    """Run the Biblio UNAM chunking and classification benchmark harness."""
    output_dir = Path(os.path.abspath(args.output_dir))
    candidate_ids = [candidate.strip() for candidate in args.candidates.split(",") if candidate.strip()]
    classifier_ids = [classifier.strip() for classifier in args.classifiers.split(",") if classifier.strip()]
    family_ids = [family_id.strip() for family_id in args.family_ids.split(",") if family_id.strip()] if args.family_ids else None
    results = run_biblio_benchmark(
        output_dir=output_dir,
        data_root=Path(args.data_root),
        smoke_size=args.smoke_size,
        tuning_size=args.tuning_size,
        holdout_size=args.holdout_size,
        seed=args.seed,
        candidate_ids=candidate_ids,
        classifier_ids=classifier_ids,
        family_ids=family_ids,
        include_optional=args.include_optional,
        include_pdf_info=not args.skip_pdf_info,
    )
    print(json.dumps(results, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description="Scraper Pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run a scraper pipeline")
    run_parser.add_argument("source", choices=["scjn", "bjv", "cas", "dof"])
    run_parser.add_argument("--max-results", type=int, default=100)
    run_parser.add_argument("--wait", action="store_true", help="Wait for completion")
    run_parser.add_argument("--use-crawl4ai", action="store_true",
                           help="Use Crawl4AI direct extraction (recommended)")
    run_parser.add_argument("--download-pdfs", action="store_true",
                           help="Download PDF/Word files from sources")
    run_parser.add_argument("--upload-to-storage", action="store_true",
                           help="Upload downloaded files to Supabase Storage")
    run_parser.add_argument("--category", help="SCJN category")
    run_parser.add_argument("--scope", help="SCJN scope")
    run_parser.add_argument("--search-term", help="BJV search term")
    run_parser.add_argument("--area", help="BJV legal area")
    run_parser.add_argument("--sport", help="CAS sport filter (Football, Athletics, etc.)")
    run_parser.add_argument("--mode", help="DOF mode (today/range)")
    run_parser.set_defaults(func=cmd_run)

    # Schedule commands
    schedule_parser = subparsers.add_parser("schedule", help="Manage schedules")
    schedule_subparsers = schedule_parser.add_subparsers(dest="schedule_command")

    # schedule list
    list_parser = schedule_subparsers.add_parser("list", help="List schedules")
    list_parser.set_defaults(func=cmd_schedule_list)

    # schedule create
    create_parser = schedule_subparsers.add_parser("create", help="Create schedule")
    create_parser.add_argument("schedule_id", help="Schedule ID (e.g., scjn-daily)")
    create_parser.add_argument("source", choices=["scjn", "bjv", "cas", "dof"])
    create_parser.add_argument("cron", help="Cron expression (e.g., '0 6 * * *')")
    create_parser.add_argument("--max-results", type=int, help="Max results")
    create_parser.add_argument("--use-crawl4ai", action="store_true",
                              help="Use Crawl4AI direct extraction (recommended)")
    create_parser.set_defaults(func=cmd_schedule_create)

    # schedule delete
    delete_parser = schedule_subparsers.add_parser("delete", help="Delete schedule")
    delete_parser.add_argument("schedule_id", help="Schedule ID to delete")
    delete_parser.set_defaults(func=cmd_schedule_delete)

    # Status command
    status_parser = subparsers.add_parser("status", help="Get workflow status")
    status_parser.add_argument("workflow_id", help="Workflow ID")
    status_parser.set_defaults(func=cmd_status)

    # Mounted-disk backfill command
    backfill_parser = subparsers.add_parser(
        "backfill-mounted",
        help="Hydrate storage from mounted disk and queue embeddings",
    )
    backfill_parser.add_argument(
        "source",
        choices=["all", *SUPPORTED_BACKFILL_SOURCES],
        help="Source to process, or 'all' to run every supported source",
    )
    backfill_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum non-complete documents to process per source",
    )
    backfill_parser.add_argument(
        "--data-root",
        default=os.environ.get("SCRAPER_MOUNTED_DATA_ROOT", "/app/mounted_data"),
        help="Mounted data root visible to this process",
    )
    backfill_parser.add_argument(
        "--only-missing-storage",
        action="store_true",
        help="Skip docs that already have storage_path and only hydrate missing files",
    )
    backfill_parser.add_argument(
        "--no-bootstrap-registration",
        action="store_true",
        help="Do not scan mounted data to create missing scraper_documents rows before hydrating",
    )
    backfill_parser.add_argument(
        "--no-trigger-embedding",
        action="store_true",
        help="Do not start the embedding workflow after hydrating files",
    )
    backfill_parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait for the triggered embedding workflow to complete",
    )
    backfill_parser.add_argument(
        "--until-exhausted",
        action="store_true",
        help="Repeat hydration passes until no missing-storage docs remain or progress stalls",
    )
    backfill_parser.add_argument(
        "--max-passes",
        type=int,
        help="Maximum number of passes when --until-exhausted is enabled",
    )
    backfill_parser.add_argument(
        "--sleep-between-passes",
        type=float,
        default=0.0,
        help="Optional pause in seconds between repeated passes for the same source",
    )
    backfill_parser.add_argument(
        "--sleep-between-sources",
        type=float,
        default=0.0,
        help="Optional pause in seconds between sources when source=all",
    )
    backfill_parser.add_argument(
        "--via-temporal",
        action="store_true",
        help="Run the mounted-data drain as a durable Temporal workflow instead of inline",
    )
    backfill_parser.set_defaults(func=cmd_backfill_mounted)

    benchmark_parser = subparsers.add_parser(
        "benchmark-cas",
        help="Benchmark CAS/TAS extraction and chunking strategies",
    )
    benchmark_parser.add_argument(
        "--output-dir",
        default="/tmp/cas_chunk_benchmark",
        help="Directory where benchmark artifacts will be written",
    )
    benchmark_parser.add_argument(
        "--data-root",
        default=os.environ.get("SCRAPER_MOUNTED_DATA_ROOT", "/app/mounted_data"),
        help="Mounted corpus root containing cas_pdfs and cas/converted",
    )
    benchmark_parser.add_argument(
        "--smoke-size",
        type=int,
        default=40,
        help="Number of documents in the smoke split",
    )
    benchmark_parser.add_argument(
        "--tuning-size",
        type=int,
        default=120,
        help="Number of documents in the tuning split",
    )
    benchmark_parser.add_argument(
        "--holdout-size",
        type=int,
        default=60,
        help="Number of documents in the holdout split",
    )
    benchmark_parser.add_argument(
        "--seed",
        type=int,
        default=20260412,
        help="Random seed for stratified sampling",
    )
    benchmark_parser.add_argument(
        "--mode",
        choices=["artifact_chunking", "raw_pdf_extraction", "full_decision"],
        default="artifact_chunking",
        help="Benchmark lane to run: converted-artifact chunking, raw-PDF extraction, or both",
    )
    benchmark_parser.add_argument(
        "--candidate-set",
        choices=["control", "unstructured", "hybrid", "all"],
        default="all",
        help="Predefined candidate bundle to run when --candidates is not provided",
    )
    benchmark_parser.add_argument(
        "--candidates",
        default="",
        help=(
            "Optional comma-separated candidate ids to run. If omitted, --candidate-set is used. "
            f"Core defaults are {', '.join(DEFAULT_CORE_CANDIDATES)}"
        ),
    )
    benchmark_parser.add_argument(
        "--difficult-subset-size",
        type=int,
        help="Optional size of the difficult raw-PDF subset before splitting into smoke/tuning/holdout",
    )
    benchmark_parser.add_argument(
        "--include-optional",
        action="store_true",
        help=f"Include optional candidates: {', '.join(OPTIONAL_CANDIDATES)}",
    )
    benchmark_parser.add_argument(
        "--doc-ids",
        help="Optional comma-separated list of exact CAS document ids to benchmark",
    )
    benchmark_parser.add_argument(
        "--skip-pdf-info",
        action="store_true",
        help="Skip pdfinfo page-count profiling during inventory generation",
    )
    benchmark_parser.set_defaults(func=cmd_benchmark_cas)

    benchmark_biblio_parser = subparsers.add_parser(
        "benchmark-biblio",
        help="Benchmark Biblio UNAM extraction, chunking, deduplication, and classification strategies",
    )
    benchmark_biblio_parser.add_argument(
        "--output-dir",
        default="/tmp/biblio_chunk_benchmark",
        help="Directory where benchmark artifacts will be written",
    )
    benchmark_biblio_parser.add_argument(
        "--data-root",
        default=os.environ.get("SCRAPER_MOUNTED_DATA_ROOT", "/app/mounted_data"),
        help="Mounted corpus root containing books/<book_id>",
    )
    benchmark_biblio_parser.add_argument(
        "--smoke-size",
        type=int,
        default=60,
        help="Number of families in the smoke split",
    )
    benchmark_biblio_parser.add_argument(
        "--tuning-size",
        type=int,
        default=140,
        help="Number of families in the tuning split",
    )
    benchmark_biblio_parser.add_argument(
        "--holdout-size",
        type=int,
        default=60,
        help="Number of families in the holdout split",
    )
    benchmark_biblio_parser.add_argument(
        "--seed",
        type=int,
        default=20260412,
        help="Random seed for stratified sampling",
    )
    benchmark_biblio_parser.add_argument(
        "--candidates",
        default=",".join(BIBLIO_DEFAULT_CORE_CANDIDATES),
        help=(
            "Comma-separated chunking candidate ids to run. Core defaults are "
            f"{', '.join(BIBLIO_DEFAULT_CORE_CANDIDATES)}"
        ),
    )
    benchmark_biblio_parser.add_argument(
        "--classifiers",
        default=",".join(BIBLIO_DEFAULT_CLASSIFIERS),
        help=(
            "Comma-separated classifier ids to run. Defaults are "
            f"{', '.join(BIBLIO_DEFAULT_CLASSIFIERS)}"
        ),
    )
    benchmark_biblio_parser.add_argument(
        "--include-optional",
        action="store_true",
        help=(
            "Include optional chunking/classifier candidates: "
            f"{', '.join(BIBLIO_OPTIONAL_CANDIDATES + BIBLIO_OPTIONAL_CLASSIFIERS)}"
        ),
    )
    benchmark_biblio_parser.add_argument(
        "--family-ids",
        help="Optional comma-separated list of exact Biblio family/book ids to benchmark",
    )
    benchmark_biblio_parser.add_argument(
        "--skip-pdf-info",
        action="store_true",
        help="Skip pdfinfo profiling during inventory generation",
    )
    benchmark_biblio_parser.set_defaults(func=cmd_benchmark_biblio)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "schedule" and not args.schedule_command:
        schedule_parser.print_help()
        sys.exit(1)

    asyncio.run(args.func(args))


if __name__ == "__main__":
    main()
