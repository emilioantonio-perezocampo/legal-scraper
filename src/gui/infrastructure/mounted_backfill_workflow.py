"""
Temporal workflow for durable mounted-disk hydration drains.

This lets us run repeat-until-exhausted backfill passes as a durable Temporal
workflow instead of a one-off CLI loop, while still reusing the exact
mounted-disk hydration logic that feeds the embedding workflow.
"""
from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import Optional

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from src.gui.infrastructure.mounted_disk_backfill import (
        SUPPORTED_BACKFILL_SOURCES,
        backfill_source_from_mounted_disk_until_exhausted,
    )
    from src.gui.infrastructure.temporal_client import TemporalScraperClient

logger = logging.getLogger(__name__)


@activity.defn
async def run_mounted_backfill_drain(config: dict) -> dict:
    source = str(config.get("source") or "").strip().lower()
    if source not in SUPPORTED_BACKFILL_SOURCES:
        raise ValueError(f"Unsupported mounted backfill source: {source}")

    result = await backfill_source_from_mounted_disk_until_exhausted(
        source=source,
        limit=int(config.get("limit", 5000) or 5000),
        include_existing_storage=bool(config.get("include_existing_storage", False)),
        bootstrap_missing_documents=bool(config.get("bootstrap_missing_documents", True)),
        trigger_embedding=bool(config.get("trigger_embedding", True)),
        wait_for_workflow=bool(config.get("wait_for_workflow", False)),
        data_root=config.get("data_root"),
        max_passes=config.get("max_passes"),
        sleep_between_passes=float(config.get("sleep_between_passes", 0.0) or 0.0),
    )
    return result.to_dict()


@activity.defn
async def trigger_mounted_backfill_drain_workflow(config: dict) -> Optional[str]:
    source = str(config.get("source") or "").strip().lower()
    if source not in SUPPORTED_BACKFILL_SOURCES:
        raise ValueError(f"Unsupported mounted backfill source: {source}")

    client = TemporalScraperClient()
    return await client.trigger_mounted_backfill_workflow(
        source=source,
        limit=int(config.get("limit", 5000) or 5000),
        include_existing_storage=bool(config.get("include_existing_storage", False)),
        bootstrap_missing_documents=bool(config.get("bootstrap_missing_documents", True)),
        trigger_embedding=bool(config.get("trigger_embedding", True)),
        data_root=config.get("data_root"),
        max_passes=config.get("max_passes"),
        sleep_between_passes=float(config.get("sleep_between_passes", 0.0) or 0.0),
    )


@workflow.defn
class MountedBackfillDrainWorkflow:
    """Durable mounted-data hydration workflow for a single source."""

    @workflow.run
    async def run(self, config: dict) -> dict:
        source = str(config.get("source") or "").strip().lower()
        workflow.logger.info("Starting mounted backfill drain for %s", source)
        result = await workflow.execute_activity(
            run_mounted_backfill_drain,
            args=[config],
            start_to_close_timeout=timedelta(hours=12),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        workflow.logger.info("Mounted backfill drain complete for %s: %s", source, result)
        return result


async def create_mounted_backfill_schedule(
    client,
    schedule_id: str,
    source: str,
    cron: str,
    config: Optional[dict] = None,
) -> str:
    """Create a Temporal schedule for recurring mounted-data hydration drains."""
    from temporalio.client import Schedule, ScheduleActionStartWorkflow, ScheduleSpec

    if source not in SUPPORTED_BACKFILL_SOURCES:
        raise ValueError(f"Unsupported mounted backfill source: {source}")

    workflow_config = {
        "source": source,
        "limit": 5000,
        "include_existing_storage": False,
        "bootstrap_missing_documents": True,
        "trigger_embedding": True,
        **(config or {}),
    }
    task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "scraper-pipeline")

    await client.create_schedule(
        schedule_id,
        Schedule(
            action=ScheduleActionStartWorkflow(
                MountedBackfillDrainWorkflow.run,
                args=[workflow_config],
                id=f"{schedule_id}-{{{{.ScheduledTime.Format `20060102-150405`}}}}",
                task_queue=task_queue,
            ),
            spec=ScheduleSpec(cron_expressions=[cron]),
        ),
    )

    logger.info(
        "Created mounted-backfill schedule %s for %s with cron %s",
        schedule_id,
        source,
        cron,
    )
    return schedule_id
