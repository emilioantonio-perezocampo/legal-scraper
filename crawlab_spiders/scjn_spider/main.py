#!/usr/bin/env python3
"""SCJN Spider for Crawlab.

This spider triggers SCJN (Suprema Corte de Justicia de la Nación) scraping
via the Legal Scraper API and reports results back to Crawlab.

Environment Variables:
    SCJN_CATEGORY: Legislation category (LEY, CODIGO, REGLAMENTO, DECRETO,
                   ACUERDO, CONSTITUCION)
    SCJN_SCOPE: Scope filter (FEDERAL or ESTATAL)
    SCJN_MAX_RESULTS: Maximum number of documents to scrape (default: 100)
    SCJN_OUTPUT_DIRECTORY: Output directory (default: scjn_data)
    LEGAL_SCRAPER_API_URL: Base URL of the legal-scraper API
    LEGAL_SCRAPER_AUTH_TOKEN: Optional JWT token for authentication
"""

import os
import sys
import logging
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.api_client import LegalScraperAPIClient, LegalScraperAPIError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("scjn_spider")


def save_item(item: dict) -> None:
    """Save an item to Crawlab's result store.

    Crawlab injects this function at runtime. This is a fallback
    for local testing.
    """
    try:
        from crawlab import save_item as crawlab_save_item

        crawlab_save_item(item)
    except ImportError:
        logger.info("Crawlab not available, logging item: %s", item)


def main() -> int:
    """Run the SCJN spider.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    logger.info("=" * 60)
    logger.info("SCJN Spider Starting")
    logger.info("=" * 60)

    # Read configuration from environment
    category = os.getenv("SCJN_CATEGORY")
    scope = os.getenv("SCJN_SCOPE")
    max_results = int(os.getenv("SCJN_MAX_RESULTS", "100"))
    output_directory = os.getenv("SCJN_OUTPUT_DIRECTORY", "scjn_data")

    logger.info("Configuration:")
    logger.info("  Category: %s", category or "(all)")
    logger.info("  Scope: %s", scope or "(all)")
    logger.info("  Max Results: %d", max_results)
    logger.info("  Output Directory: %s", output_directory)

    # Initialize API client
    client = LegalScraperAPIClient()

    # Health check
    try:
        health = client.health_check()
        logger.info("API Health: %s", health.get("status", "unknown"))
    except LegalScraperAPIError as e:
        logger.error("API health check failed: %s", e)
        save_item({
            "source": "scjn",
            "status": "error",
            "error": f"API health check failed: {e}",
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1

    # Check current status - don't start if already running
    try:
        current_status = client.get_scjn_status()
        current_state = current_status.get("status", "idle").lower()
        if current_state in ("running", "paused"):
            logger.warning("SCJN job already in progress (status: %s)", current_state)
            save_item({
                "source": "scjn",
                "status": "skipped",
                "reason": f"Job already in progress (status: {current_state})",
                "timestamp": datetime.utcnow().isoformat(),
            })
            return 0
    except LegalScraperAPIError as e:
        logger.warning("Could not check current status: %s", e)

    # Start the scraping job
    start_time = datetime.utcnow()
    try:
        result = client.start_scjn_job(
            category=category,
            scope=scope,
            max_results=max_results,
            output_directory=output_directory,
        )
        job_id = result.get("job_id", "unknown")
        logger.info("SCJN job started with ID: %s", job_id)
    except LegalScraperAPIError as e:
        logger.error("Failed to start SCJN job: %s", e)
        save_item({
            "source": "scjn",
            "status": "error",
            "error": f"Failed to start job: {e}",
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1

    # Wait for completion
    try:
        final_result = client.wait_for_completion(
            source="scjn",
            job_id=job_id,
            poll_interval=10,
            max_attempts=360,  # 1 hour max
        )
        end_time = datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()

        logger.info("SCJN job completed:")
        logger.info("  Status: %s", final_result.status)
        logger.info("  Downloaded: %d documents", final_result.downloaded_count)
        logger.info("  Errors: %d", final_result.error_count)
        logger.info("  Duration: %.1f seconds", duration_seconds)
        logger.info("  Embedding Status: %s", final_result.embedding_status or "N/A")

        # Fetch logs for debugging
        logs_response = client.get_logs("scjn", limit=50)
        logs = logs_response.get("logs", [])
        if logs:
            logger.info("  Retrieved %d log entries", len(logs))

        # Save result to Crawlab
        save_item({
            "source": "scjn",
            "job_id": job_id,
            "status": final_result.status,
            "category": category,
            "scope": scope,
            "max_results": max_results,
            "downloaded_count": final_result.downloaded_count,
            "error_count": final_result.error_count,
            "duration_seconds": duration_seconds,
            "embedding_status": final_result.embedding_status,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "timestamp": datetime.utcnow().isoformat(),
            "logs": logs,
        })

        return 0 if final_result.status == "completed" else 1

    except TimeoutError as e:
        logger.error("Job timed out: %s", e)
        save_item({
            "source": "scjn",
            "job_id": job_id,
            "status": "timeout",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1
    except LegalScraperAPIError as e:
        logger.error("Error while waiting for job: %s", e)
        save_item({
            "source": "scjn",
            "job_id": job_id,
            "status": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1


if __name__ == "__main__":
    sys.exit(main())
