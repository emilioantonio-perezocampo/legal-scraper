#!/usr/bin/env python3
"""BJV Spider for Crawlab.

This spider triggers BJV (Biblioteca Jurídica Virtual UNAM) scraping
via the Legal Scraper API and reports results back to Crawlab.

Environment Variables:
    BJV_SEARCH_TERM: Search term for filtering documents
    BJV_AREA: Legal area filter (civil, penal, constitucional, etc.)
    BJV_MAX_RESULTS: Maximum number of documents to scrape (default: 100)
    BJV_INCLUDE_CHAPTERS: Whether to include chapter content (default: true)
    BJV_DOWNLOAD_PDFS: Whether to download PDF files (default: true)
    BJV_OUTPUT_DIRECTORY: Output directory (default: bjv_data)
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
logger = logging.getLogger("bjv_spider")


def save_item(item: dict) -> None:
    """Save an item to Crawlab's result store."""
    try:
        from crawlab import save_item as crawlab_save_item

        crawlab_save_item(item)
    except ImportError:
        logger.info("Crawlab not available, logging item: %s", item)


def parse_bool(value: str, default: bool = True) -> bool:
    """Parse a boolean environment variable."""
    if not value:
        return default
    return value.lower() in ("true", "1", "yes", "on")


def main() -> int:
    """Run the BJV spider.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    logger.info("=" * 60)
    logger.info("BJV Spider Starting")
    logger.info("=" * 60)

    # Read configuration from environment
    search_term = os.getenv("BJV_SEARCH_TERM")
    area = os.getenv("BJV_AREA")
    max_results = int(os.getenv("BJV_MAX_RESULTS", "100"))
    include_chapters = parse_bool(os.getenv("BJV_INCLUDE_CHAPTERS", "true"))
    download_pdfs = parse_bool(os.getenv("BJV_DOWNLOAD_PDFS", "true"))
    output_directory = os.getenv("BJV_OUTPUT_DIRECTORY", "bjv_data")

    logger.info("Configuration:")
    logger.info("  Search Term: %s", search_term or "(none)")
    logger.info("  Area: %s", area or "(all)")
    logger.info("  Max Results: %d", max_results)
    logger.info("  Include Chapters: %s", include_chapters)
    logger.info("  Download PDFs: %s", download_pdfs)
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
            "source": "bjv",
            "status": "error",
            "error": f"API health check failed: {e}",
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1

    # Check current status - don't start if already running
    try:
        current_status = client.get_bjv_status()
        current_state = current_status.get("status", "idle").lower()
        if current_state in ("running", "paused"):
            logger.warning("BJV job already in progress (status: %s)", current_state)
            save_item({
                "source": "bjv",
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
        result = client.start_bjv_job(
            search_term=search_term,
            area=area,
            max_results=max_results,
            include_chapters=include_chapters,
            download_pdfs=download_pdfs,
            output_directory=output_directory,
        )
        job_id = result.get("job_id", "unknown")
        logger.info("BJV job started with ID: %s", job_id)
    except LegalScraperAPIError as e:
        logger.error("Failed to start BJV job: %s", e)
        save_item({
            "source": "bjv",
            "status": "error",
            "error": f"Failed to start job: {e}",
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1

    # Wait for completion
    try:
        final_result = client.wait_for_completion(
            source="bjv",
            job_id=job_id,
            poll_interval=10,
            max_attempts=360,  # 1 hour max
        )
        end_time = datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()

        logger.info("BJV job completed:")
        logger.info("  Status: %s", final_result.status)
        logger.info("  Downloaded: %d documents", final_result.downloaded_count)
        logger.info("  Errors: %d", final_result.error_count)
        logger.info("  Duration: %.1f seconds", duration_seconds)
        logger.info("  Embedding Status: %s", final_result.embedding_status or "N/A")

        # Fetch logs for debugging
        logs_response = client.get_logs("bjv", limit=50)
        logs = logs_response.get("logs", [])
        if logs:
            logger.info("  Retrieved %d log entries", len(logs))

        # Save result to Crawlab
        save_item({
            "source": "bjv",
            "job_id": job_id,
            "status": final_result.status,
            "search_term": search_term,
            "area": area,
            "max_results": max_results,
            "include_chapters": include_chapters,
            "download_pdfs": download_pdfs,
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
            "source": "bjv",
            "job_id": job_id,
            "status": "timeout",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1
    except LegalScraperAPIError as e:
        logger.error("Error while waiting for job: %s", e)
        save_item({
            "source": "bjv",
            "job_id": job_id,
            "status": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1


if __name__ == "__main__":
    sys.exit(main())
