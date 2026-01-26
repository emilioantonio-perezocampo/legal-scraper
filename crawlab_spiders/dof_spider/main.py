#!/usr/bin/env python3
"""DOF Spider for Crawlab.

This spider triggers DOF (Diario Oficial de la Federación) scraping
via the Legal Scraper API and reports results back to Crawlab.

Environment Variables:
    DOF_MODE: Scraping mode ('today' or 'range')
    DOF_START_DATE: Start date for range mode (YYYY-MM-DD)
    DOF_END_DATE: End date for range mode (YYYY-MM-DD)
    DOF_SECTION: Section filter (primera, segunda, tercera, cuarta, quinta)
    DOF_DOWNLOAD_PDFS: Whether to download PDF files (default: true)
    DOF_OUTPUT_DIRECTORY: Output directory (default: dof_data)
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
logger = logging.getLogger("dof_spider")


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
    """Run the DOF spider.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    logger.info("=" * 60)
    logger.info("DOF Spider Starting")
    logger.info("=" * 60)

    # Read configuration from environment
    mode = os.getenv("DOF_MODE", "today")
    start_date = os.getenv("DOF_START_DATE")
    end_date = os.getenv("DOF_END_DATE")
    section = os.getenv("DOF_SECTION")
    download_pdfs = parse_bool(os.getenv("DOF_DOWNLOAD_PDFS", "true"))
    output_directory = os.getenv("DOF_OUTPUT_DIRECTORY", "dof_data")

    # Validate mode
    if mode not in ("today", "range"):
        logger.error("Invalid DOF_MODE: %s. Must be 'today' or 'range'", mode)
        save_item({
            "source": "dof",
            "status": "error",
            "error": f"Invalid DOF_MODE: {mode}",
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1

    # Validate date range
    if mode == "range" and (not start_date or not end_date):
        logger.error("DOF_START_DATE and DOF_END_DATE are required for range mode")
        save_item({
            "source": "dof",
            "status": "error",
            "error": "DOF_START_DATE and DOF_END_DATE are required for range mode",
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1

    logger.info("Configuration:")
    logger.info("  Mode: %s", mode)
    if mode == "range":
        logger.info("  Start Date: %s", start_date)
        logger.info("  End Date: %s", end_date)
    logger.info("  Section: %s", section or "(all)")
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
            "source": "dof",
            "status": "error",
            "error": f"API health check failed: {e}",
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1

    # Check current status - don't start if already running
    try:
        current_status = client.get_dof_status()
        current_state = current_status.get("status", "idle").lower()
        if current_state in ("running", "paused"):
            logger.warning("DOF job already in progress (status: %s)", current_state)
            save_item({
                "source": "dof",
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
        result = client.start_dof_job(
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            section=section,
            download_pdfs=download_pdfs,
            output_directory=output_directory,
        )
        job_id = result.get("job_id", "unknown")
        logger.info("DOF job started with ID: %s", job_id)
    except LegalScraperAPIError as e:
        logger.error("Failed to start DOF job: %s", e)
        save_item({
            "source": "dof",
            "status": "error",
            "error": f"Failed to start job: {e}",
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1

    # Wait for completion
    try:
        final_result = client.wait_for_completion(
            source="dof",
            job_id=job_id,
            poll_interval=10,
            max_attempts=360,  # 1 hour max
        )
        end_time = datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()

        logger.info("DOF job completed:")
        logger.info("  Status: %s", final_result.status)
        logger.info("  Downloaded: %d documents", final_result.downloaded_count)
        logger.info("  Errors: %d", final_result.error_count)
        logger.info("  Duration: %.1f seconds", duration_seconds)
        logger.info("  Embedding Status: %s", final_result.embedding_status or "N/A")

        # Fetch logs for debugging
        logs_response = client.get_logs("dof", limit=50)
        logs = logs_response.get("logs", [])
        if logs:
            logger.info("  Retrieved %d log entries", len(logs))

        # Save result to Crawlab
        save_item({
            "source": "dof",
            "job_id": job_id,
            "status": final_result.status,
            "mode": mode,
            "start_date": start_date,
            "end_date": end_date,
            "section": section,
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
            "source": "dof",
            "job_id": job_id,
            "status": "timeout",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1
    except LegalScraperAPIError as e:
        logger.error("Error while waiting for job: %s", e)
        save_item({
            "source": "dof",
            "job_id": job_id,
            "status": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1


if __name__ == "__main__":
    sys.exit(main())
