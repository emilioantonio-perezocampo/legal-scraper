"""
Temporal Workflow for Crawl4AI-based document extraction.

This workflow uses the Crawl4AI activities to directly extract documents
without going through the API layer, providing:
- Better observability in Temporal UI
- Direct extraction without circular dependencies
- Simpler architecture with fewer moving parts

Usage:
    # Via CLI
    python -m src.gui.infrastructure.scraper_cli run scjn --use-crawl4ai

    # Via Temporal schedule
    create_crawl4ai_schedule(client, "scjn-daily", "scjn", "0 6 * * *")
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from src.gui.infrastructure.crawl4ai_activities import (
        extract_documents_crawl4ai,
        persist_documents_to_supabase,
        download_documents_pdfs,
    )
    from src.gui.infrastructure.scraper_pipeline import (
        trigger_embedding_workflow,
        send_n8n_callback,
    )

logger = logging.getLogger(__name__)


@dataclass
class Crawl4AIJobConfig:
    """Configuration for a Crawl4AI extraction job."""
    source: str  # scjn, bjv, cas, dof
    max_results: int = 100

    # SCJN options
    category: Optional[str] = None
    scope: Optional[str] = None

    # BJV options
    search_term: Optional[str] = None
    area: Optional[str] = None

    # CAS options
    sport: Optional[str] = None
    year: Optional[int] = None

    # DOF options
    mode: str = "today"
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    # Output
    output_directory: Optional[str] = None

    # PDF/Document download options
    download_pdfs: bool = False  # Download PDF/Word files
    upload_to_storage: bool = False  # Upload to Supabase Storage

    # Post-processing
    persist_to_db: bool = True
    trigger_embedding: bool = True
    callback_url: Optional[str] = None


@workflow.defn
class Crawl4AIExtractionWorkflow:
    """
    Direct Crawl4AI extraction workflow.

    This workflow:
    1. Extracts documents using Crawl4AI-based parsers (no API calls)
    2. Optionally persists to Supabase
    3. Optionally triggers embedding workflow
    4. Sends n8n callback if configured

    Benefits over API-based workflow:
    - No circular API calls (Temporal -> API -> Temporal)
    - Direct extraction with full visibility
    - Simpler error handling and retries
    - Easier to test and debug
    """

    @workflow.run
    async def run(self, config: dict) -> dict:
        """
        Execute Crawl4AI extraction workflow.

        Args:
            config: Crawl4AIJobConfig as dict

        Returns:
            Result dict with extraction stats
        """
        source = config.get("source", "unknown")
        workflow.logger.info(f"Starting Crawl4AI extraction for {source}")

        # Step 1: Extract documents using Crawl4AI
        extraction_result = await workflow.execute_activity(
            extract_documents_crawl4ai,
            args=[config],
            start_to_close_timeout=timedelta(minutes=30),  # Longer timeout for web scraping
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=10),
                maximum_interval=timedelta(minutes=5),
                maximum_attempts=3,
                non_retryable_error_types=["ValueError"],
            ),
        )

        workflow.logger.info(
            f"Extraction complete: {extraction_result.get('document_count', 0)} documents"
        )

        # Check for extraction failure
        if not extraction_result.get("success", False):
            workflow.logger.error(f"Extraction failed: {extraction_result.get('errors')}")
            return {
                "source": source,
                "success": False,
                "document_count": 0,
                "errors": extraction_result.get("errors", []),
            }

        document_count = extraction_result.get("document_count", 0)
        documents = extraction_result.get("documents", [])

        # Step 2: Download PDF/Word files (if enabled)
        download_result = None
        if config.get("download_pdfs", False) and document_count > 0:
            workflow.logger.info(f"Downloading PDFs for {document_count} documents")
            download_result = await workflow.execute_activity(
                download_documents_pdfs,
                args=[
                    documents,
                    source,
                    config.get("output_directory", f"{source}_data"),
                    config.get("upload_to_storage", False),
                ],
                start_to_close_timeout=timedelta(minutes=60),  # Longer timeout for downloads
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=10),
                    maximum_interval=timedelta(minutes=5),
                    maximum_attempts=2,  # Fewer retries for downloads
                ),
            )
            workflow.logger.info(
                f"Download result: {download_result.get('downloaded_count', 0)} downloaded, "
                f"{download_result.get('failed_count', 0)} failed"
            )

        # Step 3: Persist to Supabase (if enabled)
        persistence_result = None
        if config.get("persist_to_db", True) and document_count > 0:
            workflow.logger.info(f"Persisting {document_count} documents to Supabase")
            persistence_result = await workflow.execute_activity(
                persist_documents_to_supabase,
                args=[documents, source],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=5),
                    maximum_attempts=3,
                ),
            )
            workflow.logger.info(f"Persistence result: {persistence_result}")

        # Step 4: Trigger embedding workflow (if enabled)
        embedding_workflow_id = None
        if config.get("trigger_embedding", True) and document_count > 0:
            workflow.logger.info("Triggering embedding workflow")
            embedding_workflow_id = await workflow.execute_activity(
                trigger_embedding_workflow,
                args=[
                    f"crawl4ai-{source}-{workflow.info().workflow_id}",
                    source,
                    document_count,
                ],
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=5),
                    maximum_attempts=3,
                ),
            )
            if embedding_workflow_id:
                workflow.logger.info(f"Started embedding workflow: {embedding_workflow_id}")

        # Build result
        result = {
            "source": source,
            "success": True,
            "document_count": document_count,
            "output_directory": extraction_result.get("output_directory"),
            "errors": extraction_result.get("errors", []),
            "download": download_result,
            "persistence": persistence_result,
            "embedding_workflow_id": embedding_workflow_id,
        }

        # Step 5: Send n8n callback (if configured)
        callback_url = config.get("callback_url")
        if callback_url:
            workflow.logger.info(f"Sending n8n callback to {callback_url}")
            await workflow.execute_activity(
                send_n8n_callback,
                args=[
                    callback_url,
                    workflow.info().workflow_id,
                    source,
                    "completed" if result["success"] else "failed",
                    document_count,
                    len(extraction_result.get("errors", [])),
                    embedding_workflow_id,
                ],
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=2),
                    maximum_attempts=3,
                ),
            )

        workflow.logger.info(f"Crawl4AI workflow complete: {result}")
        return result


# =============================================================================
# Schedule Management
# =============================================================================

async def create_crawl4ai_schedule(
    client,
    schedule_id: str,
    source: str,
    cron: str,
    config: Optional[dict] = None,
) -> str:
    """
    Create a Temporal schedule for automated Crawl4AI extraction.

    Args:
        client: Temporal client
        schedule_id: Unique schedule ID (e.g., "scjn-crawl4ai-daily")
        source: Source type (scjn, bjv, cas, dof)
        cron: Cron expression (e.g., "0 6 * * *" for daily at 6 AM)
        config: Optional additional config options

    Returns:
        Schedule ID
    """
    import os
    from temporalio.client import Schedule, ScheduleActionStartWorkflow, ScheduleSpec

    workflow_config = {"source": source, **(config or {})}

    # Set defaults based on source
    defaults = {
        "scjn": {"max_results": 100, "output_directory": "scjn_data"},
        "bjv": {"max_results": 50, "output_directory": "bjv_data"},
        "cas": {"max_results": 100, "output_directory": "cas_data", "sport": "Football"},
        "dof": {"mode": "today", "output_directory": "dof_data"},
    }

    for key, value in defaults.get(source, {}).items():
        workflow_config.setdefault(key, value)

    task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "scraper-pipeline")

    await client.create_schedule(
        schedule_id,
        Schedule(
            action=ScheduleActionStartWorkflow(
                Crawl4AIExtractionWorkflow.run,
                args=[workflow_config],
                id=f"{schedule_id}-{{{{.ScheduledTime.Format `20060102-150405`}}}}",
                task_queue=task_queue,
            ),
            spec=ScheduleSpec(cron_expressions=[cron]),
        ),
    )

    logger.info(f"Created Crawl4AI schedule {schedule_id} for {source} with cron: {cron}")
    return schedule_id
