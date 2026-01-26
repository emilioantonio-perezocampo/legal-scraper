#!/usr/bin/env python3
"""CAS Spider for Crawlab.

This spider triggers CAS (Court of Arbitration for Sport) scraping
via the Legal Scraper API and reports results back to Crawlab.

Environment Variables:
    CAS_YEAR_FROM: Start year filter (default: 1986)
    CAS_YEAR_TO: End year filter (default: current year)
    CAS_SPORT: Sport filter (football, athletics, cycling, etc.)
    CAS_MATTER: Matter filter (doping, eligibility, transfer, etc.)
    CAS_MAX_RESULTS: Maximum number of documents to scrape (default: 100)
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
logger = logging.getLogger("cas_spider")


def save_item(item: dict) -> None:
    """Save an item to Crawlab's result store."""
    try:
        from crawlab import save_item as crawlab_save_item

        crawlab_save_item(item)
    except ImportError:
        logger.info("Crawlab not available, logging item: %s", item)


def parse_int(value: str, default: int = None) -> int:
    """Parse an integer environment variable."""
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def main() -> int:
    """Run the CAS spider.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    logger.info("=" * 60)
    logger.info("CAS Spider Starting")
    logger.info("=" * 60)

    # Read configuration from environment
    year_from = parse_int(os.getenv("CAS_YEAR_FROM"))
    year_to = parse_int(os.getenv("CAS_YEAR_TO"))
    sport = os.getenv("CAS_SPORT")
    matter = os.getenv("CAS_MATTER")
    max_results = int(os.getenv("CAS_MAX_RESULTS", "100"))

    logger.info("Configuration:")
    logger.info("  Year From: %s", year_from if year_from else "(default)")
    logger.info("  Year To: %s", year_to if year_to else "(default)")
    logger.info("  Sport: %s", sport or "(all)")
    logger.info("  Matter: %s", matter or "(all)")
    logger.info("  Max Results: %d", max_results)

    # Initialize API client
    client = LegalScraperAPIClient()

    # Health check
    try:
        health = client.health_check()
        logger.info("API Health: %s", health.get("status", "unknown"))
    except LegalScraperAPIError as e:
        logger.error("API health check failed: %s", e)
        save_item({
            "source": "cas",
            "status": "error",
            "error": f"API health check failed: {e}",
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1

    # Check current status - don't start if already running
    try:
        current_status = client.get_cas_status()
        current_state = current_status.get("status", "idle").lower()
        if current_state in ("running", "paused"):
            logger.warning("CAS job already in progress (status: %s)", current_state)
            save_item({
                "source": "cas",
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
        result = client.start_cas_job(
            year_from=year_from,
            year_to=year_to,
            sport=sport,
            matter=matter,
            max_results=max_results,
        )
        job_id = result.get("job_id", "unknown")
        logger.info("CAS job started with ID: %s", job_id)
    except LegalScraperAPIError as e:
        logger.error("Failed to start CAS job: %s", e)
        save_item({
            "source": "cas",
            "status": "error",
            "error": f"Failed to start job: {e}",
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1

    # Wait for completion
    try:
        final_result = client.wait_for_completion(
            source="cas",
            job_id=job_id,
            poll_interval=10,
            max_attempts=360,  # 1 hour max
        )
        end_time = datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()

        logger.info("CAS job completed:")
        logger.info("  Status: %s", final_result.status)
        logger.info("  Downloaded: %d documents", final_result.downloaded_count)
        logger.info("  Errors: %d", final_result.error_count)
        logger.info("  Duration: %.1f seconds", duration_seconds)
        logger.info("  Embedding Status: %s", final_result.embedding_status or "N/A")

        # Fetch logs for debugging
        logs_response = client.get_logs("cas", limit=50)
        logs = logs_response.get("logs", [])
        if logs:
            logger.info("  Retrieved %d log entries", len(logs))

        # Save result to Crawlab
        save_item({
            "source": "cas",
            "job_id": job_id,
            "status": final_result.status,
            "year_from": year_from,
            "year_to": year_to,
            "sport": sport,
            "matter": matter,
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
            "source": "cas",
            "job_id": job_id,
            "status": "timeout",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1
    except LegalScraperAPIError as e:
        logger.error("Error while waiting for job: %s", e)
        save_item({
            "source": "cas",
            "job_id": job_id,
            "status": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
        })
        return 1


if __name__ == "__main__":
    sys.exit(main())
