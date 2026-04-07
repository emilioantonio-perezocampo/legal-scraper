"""
Scraper Document Workflow for Temporal.

This workflow handles document embedding generation after scraping completes.
It processes documents in batches, generates embeddings using OpenRouter/OpenAI,
and stores them in Supabase for RAG search.

Workflow Input:
    job_id: The scraper job ID
    source_type: Source type (scjn, bjv, cas, dof)
    document_ids: Optional list of specific document IDs (empty = fetch all pending)
    batch_size: Documents per batch (default: 10)
    max_documents: Maximum documents to process (default: 100)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import List, Optional

from temporalio import workflow, activity

logger = logging.getLogger(__name__)


@dataclass
class DocumentEmbeddingInput:
    """Input for the ScraperDocumentWorkflow."""
    job_id: str
    source_type: str
    document_ids: List[str]
    batch_size: int = 10
    max_documents: int = 10000


@dataclass
class EmbeddingResult:
    """Result of embedding generation."""
    success: bool
    documents_processed: int
    embeddings_generated: int
    errors: List[str]


# =============================================================================
# Activities
# =============================================================================

@activity.defn
async def fetch_documents_for_embedding(
    source_type: str,
    document_ids: List[str],
    max_documents: int,
) -> List[dict]:
    """
    Fetch documents that need embedding generation.

    Args:
        source_type: Source type (scjn, bjv, cas, dof)
        document_ids: Specific IDs to fetch, or empty for all pending
        max_documents: Maximum number to fetch

    Returns:
        List of document dicts with id, title, and content
    """
    import os
    from supabase import create_client

    activity.logger.info(f"Fetching documents for embedding: source={source_type}, max={max_documents}")

    # Check if Supabase is configured
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        activity.logger.warning("Supabase not configured, returning empty list")
        return []

    client = create_client(url, key)

    if document_ids:
        # Fetch specific documents by ID
        query = client.table("scraper_documents").select("id, title, external_id, source_type")
        query = query.in_("id", document_ids)
        query = query.limit(max_documents)
        result = query.execute()
        documents = result.data if result.data else []
    else:
        # Fetch documents that DON'T already have embeddings in scraper_chunks.
        # First get IDs that already have chunks for this source.
        existing = client.table("scraper_chunks").select("document_id").eq(
            "source_type", source_type
        ).execute()
        already_embedded_ids = {row["document_id"] for row in (existing.data or [])}

        activity.logger.info(
            f"Found {len(already_embedded_ids)} already-embedded docs for {source_type}"
        )

        # Now fetch all documents for this source, ordered by created_at desc (newest first)
        query = client.table("scraper_documents").select(
            "id, title, external_id, source_type"
        ).eq("source_type", source_type).order("created_at", desc=True).limit(max_documents)
        result = query.execute()
        all_docs = result.data if result.data else []

        # Filter out already-embedded docs
        documents = [doc for doc in all_docs if doc["id"] not in already_embedded_ids]

        activity.logger.info(
            f"After filtering: {len(documents)} new docs to embed "
            f"(out of {len(all_docs)} total, {len(already_embedded_ids)} already done)"
        )

    activity.logger.info(f"Fetched {len(documents)} documents")
    return documents


@activity.defn
async def generate_and_store_embeddings(
    documents: List[dict],
    source_type: str,
) -> dict:
    """
    Generate embeddings AND store directly to Supabase in one step.

    This avoids passing large embedding vectors through Temporal's payload
    system, which has a 524KB limit that 4000-dim vectors exceed.

    Returns only a small status dict — no vectors in the payload.
    """
    import os

    activity.logger.info(f"Generating+storing embeddings for {len(documents)} docs ({source_type})")

    if not documents:
        return {"stored_count": 0, "error_count": 0}

    openrouter_key = os.environ.get("OPENROUTER_API_KEY")
    sb_url = os.environ.get("SUPABASE_URL")
    sb_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

    if not sb_url or not sb_key:
        return {"stored_count": 0, "error_count": len(documents), "error": "Supabase not configured"}

    from supabase import create_client
    sb_client = create_client(sb_url, sb_key)

    stored = 0
    errors = 0

    if openrouter_key:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            for doc in documents:
                try:
                    text = doc.get("title", "") or doc.get("external_id", "")
                    if not text:
                        errors += 1
                        continue

                    # Generate embedding
                    async with session.post(
                        "https://openrouter.ai/api/v1/embeddings",
                        headers={
                            "Authorization": f"Bearer {openrouter_key}",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": "qwen/qwen3-embedding-8b",
                            "input": text[:8000],
                        },
                    ) as resp:
                        if resp.status != 200:
                            errors += 1
                            continue
                        data = await resp.json()
                        embedding = data["data"][0]["embedding"][:4000]

                    # Store directly — vector never passes through Temporal
                    doc_id = doc["id"]
                    chunk_id = f"{source_type}-{doc_id}-0"
                    sb_client.table("scraper_chunks").upsert({
                        "chunk_id": chunk_id,
                        "document_id": doc_id,
                        "source_type": source_type,
                        "chunk_index": 0,
                        "text": text,
                        "titulo": text,
                        "embedding": embedding,
                        "metadata": {"source": "scraper_document_workflow"},
                    }, on_conflict="chunk_id").execute()
                    stored += 1

                except Exception as e:
                    activity.logger.warning(f"Error for {doc.get('id','?')}: {e}")
                    errors += 1

    activity.logger.info(f"Done: {stored} stored, {errors} errors")
    return {"stored_count": stored, "error_count": errors}


# =============================================================================
# Workflow
# =============================================================================

@workflow.defn
class ScraperDocumentWorkflow:
    """
    Workflow for processing scraped documents and generating embeddings.

    This workflow:
    1. Fetches documents that need embedding
    2. Generates embeddings in batches
    3. Stores embeddings in Supabase for RAG search
    """

    @workflow.run
    async def run(self, input: dict) -> dict:
        """
        Execute the document embedding workflow.

        Args:
            input: Dict with job_id, source_type, document_ids, batch_size, max_documents

        Returns:
            Dict with success status and processing stats
        """
        job_id = input.get("job_id", "unknown")
        source_type = input.get("source_type", "unknown")
        document_ids = input.get("document_ids", [])
        batch_size = input.get("batch_size", 10)
        max_documents = input.get("max_documents", 10000)

        workflow.logger.info(f"Starting ScraperDocumentWorkflow for job {job_id}")

        total_processed = 0
        total_embeddings = 0
        all_errors = []

        try:
            # Step 1: Fetch documents
            documents = await workflow.execute_activity(
                fetch_documents_for_embedding,
                args=[source_type, document_ids, max_documents],
                start_to_close_timeout=timedelta(minutes=5),
            )

            if not documents:
                workflow.logger.info("No documents to process")
                return {
                    "success": True,
                    "job_id": job_id,
                    "documents_processed": 0,
                    "embeddings_generated": 0,
                }

            # Step 2: Process in batches — generate + store in one activity
            # to avoid passing large embedding vectors through Temporal payloads
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]
                workflow.logger.info(f"Processing batch {i // batch_size + 1}")

                result = await workflow.execute_activity(
                    generate_and_store_embeddings,
                    args=[batch, source_type],
                    start_to_close_timeout=timedelta(minutes=10),
                )

                total_embeddings += result.get("stored_count", 0)

                total_processed += len(batch)

            workflow.logger.info(
                f"Workflow complete: processed={total_processed}, embeddings={total_embeddings}"
            )

            return {
                "success": len(all_errors) == 0,
                "job_id": job_id,
                "documents_processed": total_processed,
                "embeddings_generated": total_embeddings,
                "errors": all_errors[:10],
            }

        except Exception as e:
            workflow.logger.error(f"Workflow failed: {e}")
            return {
                "success": False,
                "job_id": job_id,
                "documents_processed": total_processed,
                "embeddings_generated": total_embeddings,
                "error": str(e),
            }
