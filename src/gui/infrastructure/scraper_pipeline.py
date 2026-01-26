"""
Temporal Scraper Pipeline - Unified workflow for scraping and embedding.

This module provides:
- ScraperPipelineWorkflow: Orchestrates scrape -> wait -> embed
- Activities for starting jobs, polling status, and triggering embedding
- Schedule management for automated runs

Architecture:
    Temporal Schedule -> ScraperPipelineWorkflow -> Scraper API -> Embedding Workflow
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Optional

from temporalio import activity, workflow
from temporalio.client import Client, Schedule, ScheduleActionStartWorkflow, ScheduleSpec
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

logger = logging.getLogger(__name__)


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class ScraperJobConfig:
    """Configuration for a scraper job."""
    source: str  # scjn, bjv, cas, dof
    max_results: int = 100
    # Source-specific options
    category: Optional[str] = None  # SCJN
    scope: Optional[str] = None  # SCJN
    search_term: Optional[str] = None  # BJV
    area: Optional[str] = None  # BJV
    year_from: Optional[int] = None  # CAS
    year_to: Optional[int] = None  # CAS
    sport: Optional[str] = None  # CAS
    matter: Optional[str] = None  # CAS
    mode: str = "today"  # DOF
    start_date: Optional[str] = None  # DOF
    end_date: Optional[str] = None  # DOF
    section: Optional[str] = None  # DOF
    output_directory: Optional[str] = None
    # n8n integration
    callback_url: Optional[str] = None  # n8n webhook URL for completion notification


@dataclass
class ScraperJobStatus:
    """Status of a scraper job."""
    job_id: str
    source: str
    status: str  # running, completed, failed, idle
    downloaded_count: int = 0
    error_count: int = 0
    progress: Optional[dict] = None


@dataclass
class PipelineResult:
    """Result of a complete pipeline run."""
    job_id: str
    source: str
    scraper_status: str
    downloaded_count: int
    error_count: int
    embedding_workflow_id: Optional[str] = None
    embedding_status: Optional[str] = None


# =============================================================================
# Activities
# =============================================================================

@activity.defn
async def start_scraper_job(config: dict) -> str:
    """
    Start a scraper job via the Legal Scraper API.

    Args:
        config: ScraperJobConfig as dict

    Returns:
        Job ID
    """
    import aiohttp

    api_url = os.environ.get("LEGAL_SCRAPER_API_URL", "http://api:8000")
    source = config["source"]

    # Build request payload based on source
    if source == "scjn":
        endpoint = f"{api_url}/api/scjn/start"
        payload = {
            "max_results": config.get("max_results", 100),
            "output_directory": config.get("output_directory", "scjn_data"),
        }
        if config.get("category"):
            payload["category"] = config["category"]
        if config.get("scope"):
            payload["scope"] = config["scope"]

    elif source == "bjv":
        endpoint = f"{api_url}/api/bjv/start"
        payload = {
            "max_resultados": config.get("max_results", 100),
            "incluir_capitulos": True,
            "descargar_pdfs": True,
            "output_directory": config.get("output_directory", "bjv_data"),
        }
        if config.get("search_term"):
            payload["termino_busqueda"] = config["search_term"]
        if config.get("area"):
            payload["area_derecho"] = config["area"]

    elif source == "cas":
        endpoint = f"{api_url}/api/cas/start"
        payload = {"max_results": config.get("max_results", 100)}
        if config.get("year_from"):
            payload["year_from"] = config["year_from"]
        if config.get("year_to"):
            payload["year_to"] = config["year_to"]
        if config.get("sport"):
            payload["sport"] = config["sport"]
        if config.get("matter"):
            payload["matter"] = config["matter"]

    elif source == "dof":
        endpoint = f"{api_url}/api/dof/start"
        payload = {
            "mode": config.get("mode", "today"),
            "download_pdfs": True,
            "output_directory": config.get("output_directory", "dof_data"),
        }
        if config.get("start_date"):
            payload["start_date"] = config["start_date"]
        if config.get("end_date"):
            payload["end_date"] = config["end_date"]
        if config.get("section"):
            payload["section"] = config["section"]
    else:
        raise ApplicationError(f"Unknown source: {source}")

    activity.logger.info(f"Starting {source} job with config: {payload}")

    async with aiohttp.ClientSession() as session:
        async with session.post(endpoint, json=payload) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise ApplicationError(f"Failed to start job: {resp.status} - {text}")
            data = await resp.json()

    if not data.get("success", True):  # Some endpoints return success field
        raise ApplicationError(f"Job start failed: {data.get('error', 'Unknown error')}")

    job_id = data.get("job_id", "unknown")
    activity.logger.info(f"Started {source} job: {job_id}")
    return job_id


@activity.defn
async def poll_scraper_status(source: str, job_id: str) -> dict:
    """
    Poll the status of a scraper job.

    Args:
        source: Source type (scjn, bjv, cas, dof)
        job_id: Job ID to check

    Returns:
        Status dict with status, downloaded_count, error_count
    """
    import aiohttp

    api_url = os.environ.get("LEGAL_SCRAPER_API_URL", "http://api:8000")
    endpoint = f"{api_url}/api/{source}/status"

    async with aiohttp.ClientSession() as session:
        async with session.get(endpoint) as resp:
            if resp.status != 200:
                raise ApplicationError(f"Failed to get status: {resp.status}")
            data = await resp.json()

    # Normalize progress fields across sources
    progress = data.get("progress", {})
    if source == "bjv":
        downloaded = progress.get("libros_descargados", 0)
        errors = progress.get("errores", 0)
    elif source == "scjn":
        downloaded = progress.get("downloaded_count", 0)
        errors = progress.get("error_count", 0)
    else:  # cas, dof
        downloaded = progress.get("downloaded", progress.get("downloaded_count", 0))
        errors = progress.get("errors", progress.get("error_count", 0))

    status = data.get("status", "unknown").lower()

    activity.logger.info(f"Job {job_id} status: {status} (downloaded: {downloaded}, errors: {errors})")

    return {
        "job_id": job_id,
        "source": source,
        "status": status,
        "downloaded_count": downloaded,
        "error_count": errors,
        "progress": progress,
    }


@activity.defn
async def trigger_embedding_workflow(job_id: str, source: str, document_count: int) -> Optional[str]:
    """
    Trigger the embedding workflow for completed documents.

    Args:
        job_id: Scraper job ID
        source: Source type
        document_count: Number of documents to embed

    Returns:
        Embedding workflow run ID, or None if skipped
    """
    if document_count == 0:
        activity.logger.info("No documents to embed, skipping")
        return None

    # Use the existing Temporal client to trigger embedding
    from src.gui.infrastructure.temporal_client import get_temporal_client

    client = get_temporal_client()
    run_id = await client.trigger_embedding_workflow(
        job_id=job_id,
        source_type=source,
        document_count=document_count,
    )

    if run_id:
        activity.logger.info(f"Started embedding workflow: {run_id}")
    else:
        activity.logger.warning("Failed to start embedding workflow")

    return run_id


@activity.defn
async def send_n8n_callback(
    callback_url: str,
    workflow_id: str,
    source: str,
    status: str,
    downloaded_count: int,
    error_count: int,
    embedding_workflow_id: Optional[str],
) -> bool:
    """
    Send completion notification to n8n webhook.

    Args:
        callback_url: n8n webhook URL
        workflow_id: Temporal workflow ID
        source: Scraper source type
        status: Final status
        downloaded_count: Documents downloaded
        error_count: Errors encountered
        embedding_workflow_id: Embedding workflow ID if started

    Returns:
        True if callback sent successfully
    """
    import aiohttp
    from datetime import datetime, timezone

    payload = {
        "event": "pipeline_completed",
        "workflow_id": workflow_id,
        "source": source,
        "status": status,
        "downloaded_count": downloaded_count,
        "error_count": error_count,
        "embedding_workflow_id": embedding_workflow_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                callback_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status < 400:
                    activity.logger.info(f"n8n callback sent successfully to {callback_url}")
                    return True
                else:
                    activity.logger.warning(f"n8n callback failed: {resp.status}")
                    return False
    except Exception as e:
        activity.logger.warning(f"n8n callback error: {e}")
        return False


# =============================================================================
# Workflow
# =============================================================================

@workflow.defn
class ScraperPipelineWorkflow:
    """
    Unified workflow that orchestrates:
    1. Starting a scraper job
    2. Polling until completion
    3. Triggering embedding workflow
    4. Sending n8n callback if configured

    This provides full visibility in Temporal UI and durable execution.
    """

    @workflow.run
    async def run(self, config: dict) -> dict:
        """
        Execute the full scraper pipeline.

        Args:
            config: ScraperJobConfig as dict

        Returns:
            PipelineResult as dict
        """
        source = config.get("source", "unknown")
        workflow.logger.info(f"Starting {source} pipeline")

        # Step 1: Start the scraper job
        job_id = await workflow.execute_activity(
            start_scraper_job,
            args=[config],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=5),
                maximum_interval=timedelta(minutes=1),
                maximum_attempts=3,
            ),
        )

        workflow.logger.info(f"Job started: {job_id}")

        # Step 2: Poll until terminal state
        terminal_states = {"completed", "failed", "cancelled", "idle"}
        max_polls = 360  # 1 hour with 10s intervals
        poll_count = 0
        final_status = None

        while poll_count < max_polls:
            status = await workflow.execute_activity(
                poll_scraper_status,
                args=[source, job_id],
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=2),
                    maximum_attempts=3,
                ),
            )

            current_state = status.get("status", "unknown")

            if current_state in terminal_states:
                final_status = status
                workflow.logger.info(f"Job completed with status: {current_state}")
                break

            poll_count += 1
            await asyncio.sleep(10)  # Wait 10 seconds between polls

        if final_status is None:
            final_status = {
                "job_id": job_id,
                "source": source,
                "status": "timeout",
                "downloaded_count": 0,
                "error_count": 0,
            }
            workflow.logger.warning("Job timed out after maximum polls")

        # Step 3: Trigger embedding if successful
        embedding_workflow_id = None
        if final_status.get("status") == "completed" and final_status.get("downloaded_count", 0) > 0:
            embedding_workflow_id = await workflow.execute_activity(
                trigger_embedding_workflow,
                args=[job_id, source, final_status["downloaded_count"]],
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=5),
                    maximum_attempts=3,
                ),
            )

        # Return final result
        result = {
            "job_id": job_id,
            "source": source,
            "scraper_status": final_status.get("status"),
            "downloaded_count": final_status.get("downloaded_count", 0),
            "error_count": final_status.get("error_count", 0),
            "embedding_workflow_id": embedding_workflow_id,
        }

        # Step 4: Send n8n callback if configured
        callback_url = config.get("callback_url")
        if callback_url:
            workflow.logger.info(f"Sending n8n callback to {callback_url}")
            await workflow.execute_activity(
                send_n8n_callback,
                args=[
                    callback_url,
                    workflow.info().workflow_id,
                    source,
                    final_status.get("status", "unknown"),
                    final_status.get("downloaded_count", 0),
                    final_status.get("error_count", 0),
                    embedding_workflow_id,
                ],
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=2),
                    maximum_attempts=3,
                ),
            )

        workflow.logger.info(f"Pipeline complete: {result}")
        return result


# =============================================================================
# Schedule Management
# =============================================================================

async def create_scraper_schedule(
    client: Client,
    schedule_id: str,
    source: str,
    cron: str,
    config: Optional[dict] = None,
) -> str:
    """
    Create a Temporal schedule for automated scraper runs.

    Args:
        client: Temporal client
        schedule_id: Unique schedule ID (e.g., "scjn-daily")
        source: Source type (scjn, bjv, cas, dof)
        cron: Cron expression (e.g., "0 6 * * *" for daily at 6 AM)
        config: Optional additional config options

    Returns:
        Schedule ID
    """
    workflow_config = {"source": source, **(config or {})}

    # Set defaults based on source
    if source == "scjn":
        workflow_config.setdefault("max_results", 100)
        workflow_config.setdefault("output_directory", "scjn_data")
    elif source == "bjv":
        workflow_config.setdefault("max_results", 50)
        workflow_config.setdefault("output_directory", "bjv_data")
    elif source == "cas":
        workflow_config.setdefault("max_results", 100)
    elif source == "dof":
        workflow_config.setdefault("mode", "today")
        workflow_config.setdefault("output_directory", "dof_data")

    task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "scraper-pipeline")

    await client.create_schedule(
        schedule_id,
        Schedule(
            action=ScheduleActionStartWorkflow(
                ScraperPipelineWorkflow.run,
                args=[workflow_config],
                id=f"{schedule_id}-{{{{.ScheduledTime.Format `20060102-150405`}}}}",
                task_queue=task_queue,
            ),
            spec=ScheduleSpec(cron_expressions=[cron]),
        ),
    )

    logger.info(f"Created schedule {schedule_id} for {source} with cron: {cron}")
    return schedule_id


async def list_scraper_schedules(client: Client) -> list:
    """List all scraper pipeline schedules."""
    schedules = []
    schedule_list = await client.list_schedules()
    async for schedule in schedule_list:
        if schedule.id.startswith(("scjn-", "bjv-", "cas-", "dof-")):
            schedules.append({
                "id": schedule.id,
                "info": schedule.info,
            })
    return schedules


async def delete_scraper_schedule(client: Client, schedule_id: str) -> bool:
    """Delete a scraper schedule."""
    try:
        handle = client.get_schedule_handle(schedule_id)
        await handle.delete()
        logger.info(f"Deleted schedule: {schedule_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to delete schedule {schedule_id}: {e}")
        return False
