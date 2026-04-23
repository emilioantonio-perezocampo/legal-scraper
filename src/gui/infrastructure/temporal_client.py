"""
Temporal Client Adapter for Scraper API.

Triggers ScraperDocumentWorkflow when a scraper job completes,
enabling durable document embedding processing.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class TemporalConfig:
    """Configuration for Temporal connection."""

    address: str
    namespace: str = "default"
    task_queue: str = "legalconnect-meeting"
    enabled: bool = True

    @classmethod
    def from_env(cls) -> "TemporalConfig":
        address = os.environ.get("TEMPORAL_ADDRESS", "").strip()
        enabled = bool(address) and os.environ.get("TEMPORAL_ENABLED", "true").lower() == "true"
        return cls(
            address=address,
            namespace=os.environ.get("TEMPORAL_NAMESPACE", "default").strip(),
            task_queue=os.environ.get("TEMPORAL_TASK_QUEUE", "legalconnect-meeting").strip(),
            enabled=enabled,
        )


class TemporalScraperClient:
    """Client for triggering ScraperDocumentWorkflow from the scraper API."""

    WORKFLOW_NAME = "ScraperDocumentWorkflow"
    WORKFLOW_ID_PREFIX = "scraper-job-"
    MOUNTED_BACKFILL_WORKFLOW_NAME = "MountedBackfillDrainWorkflow"
    MOUNTED_BACKFILL_WORKFLOW_ID_PREFIX = "mounted-backfill-drain-"
    WORKFLOW_TASK_TIMEOUT = timedelta(minutes=2)

    def __init__(self, config: Optional[TemporalConfig] = None):
        self._config = config or TemporalConfig.from_env()
        self._client = None

    @property
    def enabled(self) -> bool:
        return self._config.enabled and bool(self._config.address)

    async def _get_client(self):
        """Lazy initialization of Temporal client with connection retry."""
        if not self.enabled:
            return None

        if self._client is None:
            try:
                from temporalio.client import Client

                self._client = await Client.connect(
                    self._config.address,
                    namespace=self._config.namespace,
                )
                logger.info(f"Connected to Temporal at {self._config.address}")
            except Exception as e:
                logger.error(f"Failed to connect to Temporal: {e}")
                return None
        return self._client

    async def trigger_embedding_workflow(
        self,
        job_id: str,
        source_type: str,
        document_count: int,
        document_ids: Optional[list[str]] = None,
        batch_size: int = 10,
        max_documents: int = 0,
    ) -> Optional[str]:
        """
        Trigger ScraperDocumentWorkflow for a completed scraper job.

        Args:
            job_id: The scraper job ID
            source_type: Source type (scjn, bjv, cas, dof)
            document_count: Number of documents to process
            document_ids: Exact Supabase document UUIDs for this workflow run
            batch_size: Documents per batch
            max_documents: Maximum documents to process (0 = all unembedded)

        Returns:
            Workflow run ID if started, None if disabled or failed
        """
        if not self.enabled:
            logger.debug("Temporal integration disabled")
            return None

        document_ids = list(dict.fromkeys(document_ids or []))
        if not document_ids:
            logger.warning(
                "Skipping embedding workflow for %s because no exact document IDs were provided",
                job_id,
            )
            return None

        client = await self._get_client()
        if client is None:
            return None

        workflow_id = f"{self.WORKFLOW_ID_PREFIX}{job_id}"

        effective_max = max_documents if max_documents > 0 else (
            len(document_ids)
        )

        workflow_input = {
            "job_id": job_id,
            "source_type": source_type,
            "document_ids": document_ids,
            "batch_size": batch_size,
            "max_documents": effective_max,
        }

        try:
            handle = await client.start_workflow(
                self.WORKFLOW_NAME,
                workflow_input,
                id=workflow_id,
                task_queue=self._config.task_queue,
                task_timeout=self.WORKFLOW_TASK_TIMEOUT,
            )
            logger.info(f"Started workflow {workflow_id} for {document_count} docs")
            return handle.result_run_id

        except Exception as e:
            if "already started" in str(e).lower():
                logger.info(f"Workflow {workflow_id} already running")
                return workflow_id
            logger.error(f"Failed to start workflow: {e}")
            return None

    async def get_workflow_status(self, job_id: str) -> Optional[dict]:
        """Get status of an embedding workflow."""
        if not self.enabled:
            return None

        client = await self._get_client()
        if client is None:
            return None

        workflow_id = f"{self.WORKFLOW_ID_PREFIX}{job_id}"

        try:
            handle = client.get_workflow_handle(workflow_id)
            desc = await handle.describe()
            return {
                "workflow_id": workflow_id,
                "status": desc.status.name,
                "start_time": desc.start_time.isoformat() if desc.start_time else None,
            }
        except Exception as e:
            logger.debug(f"Workflow {workflow_id} not found: {e}")
            return None

    async def trigger_mounted_backfill_workflow(
        self,
        source: str,
        *,
        limit: int = 5000,
        include_existing_storage: bool = False,
        bootstrap_missing_documents: bool = True,
        trigger_embedding: bool = True,
        data_root: Optional[str] = None,
        max_passes: Optional[int] = None,
        sleep_between_passes: float = 0.0,
    ) -> Optional[str]:
        """Start or reuse a durable mounted-data drain workflow for a source."""
        if not self.enabled:
            logger.debug("Temporal integration disabled")
            return None

        client = await self._get_client()
        if client is None:
            return None

        workflow_id = f"{self.MOUNTED_BACKFILL_WORKFLOW_ID_PREFIX}{source}"
        workflow_input = {
            "source": source,
            "limit": limit,
            "include_existing_storage": include_existing_storage,
            "bootstrap_missing_documents": bootstrap_missing_documents,
            "trigger_embedding": trigger_embedding,
            "data_root": data_root,
            "max_passes": max_passes,
            "sleep_between_passes": sleep_between_passes,
        }

        try:
            await client.start_workflow(
                self.MOUNTED_BACKFILL_WORKFLOW_NAME,
                workflow_input,
                id=workflow_id,
                task_queue=self._config.task_queue,
                task_timeout=self.WORKFLOW_TASK_TIMEOUT,
            )
            logger.info("Started mounted backfill workflow %s", workflow_id)
            return workflow_id
        except Exception as e:
            if "already started" in str(e).lower():
                logger.info("Mounted backfill workflow %s already running", workflow_id)
                return workflow_id
            logger.error("Failed to start mounted backfill workflow %s: %s", workflow_id, e)
            return None


# Singleton instance
_temporal_client: Optional[TemporalScraperClient] = None


def get_temporal_client() -> TemporalScraperClient:
    """Get the singleton Temporal client instance."""
    global _temporal_client
    if _temporal_client is None:
        _temporal_client = TemporalScraperClient()
    return _temporal_client
