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
import os
import sys

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
