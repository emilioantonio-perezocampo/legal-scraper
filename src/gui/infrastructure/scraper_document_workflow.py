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
    max_documents: int = 100


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

    # Query documents that need embeddings
    query = client.table("scraper_documents").select("id, title, external_id, source_type")

    if document_ids:
        query = query.in_("id", document_ids)
    else:
        query = query.eq("source_type", source_type)

    query = query.limit(max_documents)

    result = query.execute()
    documents = result.data if result.data else []

    activity.logger.info(f"Fetched {len(documents)} documents")
    return documents


@activity.defn
async def generate_embeddings_batch(
    documents: List[dict],
    model: str = "text-embedding-3-small",
) -> List[dict]:
    """
    Generate embeddings for a batch of documents.

    Uses OpenRouter or falls back to sentence-transformers.

    Args:
        documents: List of documents with content
        model: Embedding model name

    Returns:
        List of dicts with document_id and embedding vector
    """
    import os

    activity.logger.info(f"Generating embeddings for {len(documents)} documents")

    if not documents:
        return []

    results = []

    # Try OpenRouter first, fallback to sentence-transformers
    openrouter_key = os.environ.get("OPENROUTER_API_KEY")

    if openrouter_key:
        # Use OpenRouter API
        import aiohttp

        async with aiohttp.ClientSession() as session:
            for doc in documents:
                try:
                    text = doc.get("title", "") or doc.get("external_id", "")
                    if not text:
                        continue

                    async with session.post(
                        "https://openrouter.ai/api/v1/embeddings",
                        headers={
                            "Authorization": f"Bearer {openrouter_key}",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": "openai/text-embedding-3-small",
                            "input": text[:8000],  # Limit input length
                        },
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            embedding = data["data"][0]["embedding"]
                            results.append({
                                "document_id": doc["id"],
                                "embedding": embedding,
                                "title": text,
                            })
                        else:
                            activity.logger.warning(f"Embedding failed for {doc['id']}: {resp.status}")
                except Exception as e:
                    activity.logger.error(f"Error generating embedding for {doc['id']}: {e}")
    else:
        # Fallback to sentence-transformers
        activity.logger.info("Using sentence-transformers fallback")
        try:
            from sentence_transformers import SentenceTransformer

            model = SentenceTransformer("all-MiniLM-L6-v2")
            texts = [doc.get("title", "") or doc.get("external_id", "") for doc in documents]
            embeddings = model.encode(texts)

            for doc, text, embedding in zip(documents, texts, embeddings):
                results.append({
                    "document_id": doc["id"],
                    "embedding": embedding.tolist(),
                    "title": text,
                })
        except ImportError:
            activity.logger.error("sentence-transformers not installed")

    activity.logger.info(f"Generated {len(results)} embeddings")
    return results


@activity.defn
async def store_embeddings(
    embeddings: List[dict],
    source_type: str,
) -> dict:
    """
    Store generated embeddings in Supabase.

    Args:
        embeddings: List of dicts with document_id and embedding
        source_type: Source type for metadata

    Returns:
        Dict with success status and counts
    """
    import os
    from supabase import create_client

    activity.logger.info(f"Storing {len(embeddings)} embeddings")

    if not embeddings:
        return {"success": True, "stored_count": 0}

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        activity.logger.warning("Supabase not configured")
        return {"success": False, "error": "Supabase not configured"}

    client = create_client(url, key)
    stored = 0
    errors = []

    for item in embeddings:
        try:
            doc_id = item["document_id"]
            embedding = item["embedding"]
            title = item.get("title", "")

            # Generate unique chunk_id for this document
            chunk_id = f"{source_type}-{doc_id}-0"

            # Insert into scraper_chunks table with embedding
            # The embedding column is halfvec(4000) type
            client.table("scraper_chunks").upsert({
                "chunk_id": chunk_id,
                "document_id": doc_id,
                "source_type": source_type,
                "chunk_index": 0,  # Single chunk per document for now
                "text": title or "Document title",  # Required field
                "titulo": title,
                "embedding": embedding,
                "metadata": {"source": "scraper_document_workflow"},
            }, on_conflict="chunk_id").execute()
            stored += 1
        except Exception as e:
            errors.append(f"{item['document_id']}: {str(e)}")

    activity.logger.info(f"Stored {stored}/{len(embeddings)} embeddings")

    return {
        "success": len(errors) == 0,
        "stored_count": stored,
        "error_count": len(errors),
        "errors": errors[:5],
    }


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
        max_documents = input.get("max_documents", 100)

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

            # Step 2: Process in batches
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]
                workflow.logger.info(f"Processing batch {i // batch_size + 1}")

                # Generate embeddings
                embeddings = await workflow.execute_activity(
                    generate_embeddings_batch,
                    args=[batch],
                    start_to_close_timeout=timedelta(minutes=10),
                )

                if embeddings:
                    # Store embeddings
                    result = await workflow.execute_activity(
                        store_embeddings,
                        args=[embeddings, source_type],
                        start_to_close_timeout=timedelta(minutes=5),
                    )

                    total_embeddings += result.get("stored_count", 0)
                    if result.get("errors"):
                        all_errors.extend(result["errors"])

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
