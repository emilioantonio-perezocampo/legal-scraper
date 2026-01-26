"""
Temporal Worker for Scraper Pipeline.

This worker handles the ScraperPipelineWorkflow and its activities.
Run this as a separate process alongside the API.

Usage:
    python -m src.gui.infrastructure.scraper_worker

Environment Variables:
    TEMPORAL_ADDRESS: Temporal server address (default: temporal-temporal-1:7233)
    TEMPORAL_NAMESPACE: Namespace (default: default)
    TEMPORAL_TASK_QUEUE: Task queue name (default: scraper-pipeline)
    LEGAL_SCRAPER_API_URL: API URL for scraper (default: http://api:8000)
"""
import asyncio
import logging
import os
import signal
import sys

from temporalio.client import Client
from temporalio.worker import Worker

# Add src to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from src.gui.infrastructure.scraper_pipeline import (
    ScraperPipelineWorkflow,
    start_scraper_job,
    poll_scraper_status,
    trigger_embedding_workflow,
    send_n8n_callback,
)
from src.gui.infrastructure.crawl4ai_workflow import Crawl4AIExtractionWorkflow
from src.gui.infrastructure.crawl4ai_activities import (
    extract_documents_crawl4ai,
    extract_scjn_documents,
    extract_dof_documents,
    extract_bjv_documents,
    extract_cas_documents,
    persist_documents_to_supabase,
    download_documents_pdfs,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def run_worker():
    """Run the Temporal worker."""
    # Configuration from environment
    temporal_address = os.environ.get("TEMPORAL_ADDRESS", "temporal-temporal-1:7233")
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")
    task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "scraper-pipeline")

    logger.info(f"Connecting to Temporal at {temporal_address}")
    logger.info(f"Namespace: {namespace}, Task Queue: {task_queue}")

    # Connect to Temporal
    client = await Client.connect(temporal_address, namespace=namespace)
    logger.info("Connected to Temporal")

    # Create worker with both API-based and Crawl4AI workflows
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[
            ScraperPipelineWorkflow,      # Original API-based workflow
            Crawl4AIExtractionWorkflow,   # New Crawl4AI direct workflow
        ],
        activities=[
            # Original API-based activities
            start_scraper_job,
            poll_scraper_status,
            trigger_embedding_workflow,
            send_n8n_callback,
            # New Crawl4AI activities
            extract_documents_crawl4ai,
            extract_scjn_documents,
            extract_dof_documents,
            extract_bjv_documents,
            extract_cas_documents,
            persist_documents_to_supabase,
            download_documents_pdfs,
        ],
    )

    logger.info(f"Starting worker on task queue: {task_queue}")

    # Handle shutdown signals
    shutdown_event = asyncio.Event()

    def handle_signal(sig):
        logger.info(f"Received signal {sig}, shutting down...")
        shutdown_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))

    # Run worker until shutdown
    async with worker:
        logger.info("Worker started, waiting for tasks...")
        await shutdown_event.wait()

    logger.info("Worker shutdown complete")


def main():
    """Entry point."""
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")


if __name__ == "__main__":
    main()
