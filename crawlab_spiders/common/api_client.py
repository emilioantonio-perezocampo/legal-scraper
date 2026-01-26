"""HTTP client for communicating with the Legal Scraper API.

This client provides methods to trigger scraper jobs and monitor their status
from Crawlab spiders.
"""

import os
import time
import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional, TypeVar
from urllib.parse import urljoin

import requests

T = TypeVar("T")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class JobResult:
    """Result from a scraper job."""

    job_id: str
    source: str
    status: str
    downloaded_count: int
    error_count: int
    embedding_status: Optional[str] = None


class LegalScraperAPIError(Exception):
    """Raised when the Legal Scraper API returns an error."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class LegalScraperAPIClient:
    """Client for the Legal Scraper API.

    This client wraps HTTP calls to the legal-scraper FastAPI application,
    providing methods for each scraper source (SCJN, BJV, CAS, DOF).
    """

    DEFAULT_TIMEOUT = 30
    DEFAULT_POLL_INTERVAL = 10
    MAX_POLL_ATTEMPTS = 360  # 1 hour with 10s intervals

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT,
        auth_token: Optional[str] = None,
    ):
        """Initialize the API client.

        Args:
            base_url: Base URL of the legal-scraper API. Defaults to
                      LEGAL_SCRAPER_API_URL env var or http://localhost:8000.
            timeout: Request timeout in seconds.
            auth_token: Optional JWT token for authenticated requests.
        """
        self.base_url = base_url or os.getenv(
            "LEGAL_SCRAPER_API_URL", "http://localhost:8000"
        )
        self.timeout = timeout
        self.auth_token = auth_token or os.getenv("LEGAL_SCRAPER_AUTH_TOKEN")
        self._session = requests.Session()

        if self.auth_token:
            self._session.headers["Authorization"] = f"Bearer {self.auth_token}"

        logger.info("LegalScraperAPIClient initialized with base_url=%s", self.base_url)

    def _request(
        self,
        method: str,
        endpoint: str,
        json: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> dict:
        """Make an HTTP request to the API.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (e.g., /api/scjn/start)
            json: JSON body for POST requests
            params: Query parameters

        Returns:
            Response JSON as dict

        Raises:
            LegalScraperAPIError: If the request fails
        """
        url = urljoin(self.base_url, endpoint)

        try:
            response = self._session.request(
                method=method,
                url=url,
                json=json,
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            error_msg = str(e)
            try:
                error_detail = response.json().get("detail", error_msg)
            except Exception:
                error_detail = error_msg
            raise LegalScraperAPIError(error_detail, response.status_code) from e
        except requests.exceptions.RequestException as e:
            raise LegalScraperAPIError(f"Request failed: {e}") from e

    def _retry_request(
        self,
        func: Callable[[], T],
        max_retries: int = 3,
        backoff: float = 2.0,
    ) -> T:
        """Retry a function with exponential backoff.

        Args:
            func: Callable that may raise LegalScraperAPIError
            max_retries: Maximum number of retry attempts
            backoff: Base for exponential backoff (sleep = backoff^attempt)

        Returns:
            Result from successful function call

        Raises:
            LegalScraperAPIError: If all retries fail
        """
        last_error = None
        for attempt in range(max_retries):
            try:
                return func()
            except LegalScraperAPIError as e:
                # Don't retry client errors (4xx except 429)
                if e.status_code and 400 <= e.status_code < 500 and e.status_code != 429:
                    raise
                last_error = e
                if attempt < max_retries - 1:
                    sleep_time = backoff ** attempt
                    logger.warning(
                        "Attempt %d/%d failed, retrying in %.1fs: %s",
                        attempt + 1,
                        max_retries,
                        sleep_time,
                        e,
                    )
                    time.sleep(sleep_time)
        raise last_error

    def _extract_progress(self, source: str, status: dict) -> tuple[int, int]:
        """Extract downloaded_count and error_count from source-specific status.

        Different scrapers use different field names for progress:
        - BJV: libros_descargados, errores
        - SCJN: discovered_count, downloaded_count, error_count
        - CAS/DOF: discovered, downloaded, errors

        Args:
            source: Source name ('scjn', 'bjv', 'cas', 'dof')
            status: Status dict from the API

        Returns:
            Tuple of (downloaded_count, error_count)
        """
        progress = status.get("progress", {})
        source_lower = source.lower()

        if source_lower == "bjv":
            return (
                progress.get("libros_descargados", 0),
                progress.get("errores", 0),
            )
        elif source_lower == "scjn":
            return (
                progress.get("downloaded_count", 0),
                progress.get("error_count", 0),
            )
        else:  # cas, dof
            return (
                progress.get("downloaded", progress.get("downloaded_count", 0)),
                progress.get("errors", progress.get("error_count", 0)),
            )

    # -------------------------------------------------------------------------
    # Health Check
    # -------------------------------------------------------------------------

    def health_check(self) -> dict:
        """Check if the API is healthy.

        Returns:
            Health status dict with 'status' key
        """
        return self._request("GET", "/api/health")

    # -------------------------------------------------------------------------
    # SCJN Scraper
    # -------------------------------------------------------------------------

    def start_scjn_job(
        self,
        category: Optional[str] = None,
        scope: Optional[str] = None,
        max_results: int = 100,
        output_directory: str = "scjn_data",
        retry: bool = True,
    ) -> dict:
        """Start an SCJN scraping job.

        Args:
            category: Legislation category (LEY, CODIGO, REGLAMENTO, DECRETO,
                      ACUERDO, CONSTITUCION)
            scope: Scope filter (FEDERAL or ESTATAL)
            max_results: Maximum number of documents to scrape
            output_directory: Directory to store scraped data
            retry: Whether to retry on transient failures (default: True)

        Returns:
            Job start response with job_id
        """
        payload = {
            "max_results": max_results,
            "output_directory": output_directory,
        }
        if category:
            payload["category"] = category
        if scope:
            payload["scope"] = scope

        logger.info("Starting SCJN job with params: %s", payload)
        if retry:
            return self._retry_request(
                lambda: self._request("POST", "/api/scjn/start", json=payload)
            )
        return self._request("POST", "/api/scjn/start", json=payload)

    def get_scjn_status(self) -> dict:
        """Get current SCJN job status.

        Returns:
            Status dict with job state, progress, and metrics
        """
        return self._request("GET", "/api/scjn/status")

    def get_scjn_categories(self) -> dict:
        """Get available SCJN legislation categories.

        Returns:
            Dict with 'categories' list
        """
        return self._request("GET", "/api/scjn/categories")

    # -------------------------------------------------------------------------
    # BJV Scraper
    # -------------------------------------------------------------------------

    def start_bjv_job(
        self,
        search_term: Optional[str] = None,
        area: Optional[str] = None,
        max_results: int = 100,
        include_chapters: bool = True,
        download_pdfs: bool = True,
        output_directory: str = "bjv_data",
        retry: bool = True,
    ) -> dict:
        """Start a BJV (Biblioteca Jurídica Virtual) scraping job.

        Args:
            search_term: Search term for filtering documents
            area: Legal area filter (civil, penal, constitucional, etc.)
            max_results: Maximum number of documents to scrape
            include_chapters: Whether to include chapter content
            download_pdfs: Whether to download PDF files
            output_directory: Directory to store scraped data
            retry: Whether to retry on transient failures (default: True)

        Returns:
            Job start response with job_id
        """
        payload = {
            "max_resultados": max_results,
            "incluir_capitulos": include_chapters,
            "descargar_pdfs": download_pdfs,
            "output_directory": output_directory,
        }
        if search_term:
            payload["termino_busqueda"] = search_term
        if area:
            payload["area_derecho"] = area

        logger.info("Starting BJV job with params: %s", payload)
        if retry:
            return self._retry_request(
                lambda: self._request("POST", "/api/bjv/start", json=payload)
            )
        return self._request("POST", "/api/bjv/start", json=payload)

    def get_bjv_status(self) -> dict:
        """Get current BJV job status.

        Returns:
            Status dict with job state, progress, and metrics
        """
        return self._request("GET", "/api/bjv/status")

    def get_bjv_areas(self) -> dict:
        """Get available BJV legal areas.

        Returns:
            Dict with 'areas' list
        """
        return self._request("GET", "/api/bjv/areas")

    # -------------------------------------------------------------------------
    # CAS Scraper
    # -------------------------------------------------------------------------

    def start_cas_job(
        self,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        sport: Optional[str] = None,
        matter: Optional[str] = None,
        max_results: int = 100,
        retry: bool = True,
    ) -> dict:
        """Start a CAS (Court of Arbitration for Sport) scraping job.

        Args:
            year_from: Start year filter (default: 1986)
            year_to: End year filter (default: current year)
            sport: Sport filter (football, athletics, cycling, etc.)
            matter: Matter filter (doping, eligibility, transfer, etc.)
            max_results: Maximum number of documents to scrape
            retry: Whether to retry on transient failures (default: True)

        Returns:
            Job start response with job_id
        """
        payload = {"max_results": max_results}
        if year_from is not None:
            payload["year_from"] = year_from
        if year_to is not None:
            payload["year_to"] = year_to
        if sport:
            payload["sport"] = sport
        if matter:
            payload["matter"] = matter

        logger.info("Starting CAS job with params: %s", payload)
        if retry:
            return self._retry_request(
                lambda: self._request("POST", "/api/cas/start", json=payload)
            )
        return self._request("POST", "/api/cas/start", json=payload)

    def get_cas_status(self) -> dict:
        """Get current CAS job status.

        Returns:
            Status dict with job state, progress, and metrics
        """
        return self._request("GET", "/api/cas/status")

    def get_cas_filters(self) -> dict:
        """Get available CAS filter options.

        Returns:
            Dict with 'sports', 'matters', and 'year_range'
        """
        return self._request("GET", "/api/cas/filters")

    # -------------------------------------------------------------------------
    # DOF Scraper
    # -------------------------------------------------------------------------

    def start_dof_job(
        self,
        mode: str = "today",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        section: Optional[str] = None,
        download_pdfs: bool = True,
        output_directory: str = "dof_data",
        retry: bool = True,
    ) -> dict:
        """Start a DOF (Diario Oficial de la Federación) scraping job.

        Args:
            mode: Scraping mode ('today' or 'range')
            start_date: Start date for range mode (YYYY-MM-DD)
            end_date: End date for range mode (YYYY-MM-DD)
            section: Section filter (primera, segunda, tercera, cuarta, quinta)
            download_pdfs: Whether to download PDF files
            output_directory: Directory to store scraped data
            retry: Whether to retry on transient failures (default: True)

        Returns:
            Job start response with job_id
        """
        payload = {
            "mode": mode,
            "download_pdfs": download_pdfs,
            "output_directory": output_directory,
        }
        if start_date:
            payload["start_date"] = start_date
        if end_date:
            payload["end_date"] = end_date
        if section:
            payload["section"] = section

        logger.info("Starting DOF job with params: %s", payload)
        if retry:
            return self._retry_request(
                lambda: self._request("POST", "/api/dof/start", json=payload)
            )
        return self._request("POST", "/api/dof/start", json=payload)

    def get_dof_status(self) -> dict:
        """Get current DOF job status.

        Returns:
            Status dict with job state, progress, and metrics
        """
        return self._request("GET", "/api/dof/status")

    def get_dof_sections(self) -> dict:
        """Get available DOF sections.

        Returns:
            Dict with 'sections' and 'modes'
        """
        return self._request("GET", "/api/dof/sections")

    # -------------------------------------------------------------------------
    # Generic Status & Polling
    # -------------------------------------------------------------------------

    def get_status(self, source: str) -> dict:
        """Get status for a specific scraper source.

        Args:
            source: Source name ('scjn', 'bjv', 'cas', 'dof')

        Returns:
            Status dict for the specified source

        Raises:
            ValueError: If source is not recognized
        """
        source_lower = source.lower()
        status_methods = {
            "scjn": self.get_scjn_status,
            "bjv": self.get_bjv_status,
            "cas": self.get_cas_status,
            "dof": self.get_dof_status,
        }

        if source_lower not in status_methods:
            raise ValueError(f"Unknown source: {source}. Must be one of {list(status_methods.keys())}")

        return status_methods[source_lower]()

    def get_embedding_status(self, job_id: str) -> Optional[str]:
        """Get the Temporal embedding workflow status for a job.

        Args:
            job_id: The job ID to check

        Returns:
            Embedding workflow status string, or None if not found
        """
        try:
            response = self._request("GET", f"/api/embedding-status/{job_id}")
            return response.get("status")
        except LegalScraperAPIError as e:
            if e.status_code == 404:
                return None
            raise

    def get_logs(
        self,
        source: str,
        limit: int = 100,
        level: Optional[str] = None,
    ) -> dict:
        """Get logs for a scraper source.

        Args:
            source: Source name ('scjn', 'bjv', 'cas', 'dof')
            limit: Maximum number of log entries to return
            level: Optional log level filter ('DEBUG', 'INFO', 'WARNING', 'ERROR')

        Returns:
            Dict containing 'logs' list with log entries
        """
        params = {"limit": limit}
        if level:
            params["level"] = level

        try:
            return self._request("GET", f"/api/{source}/logs", params=params)
        except LegalScraperAPIError as e:
            # If logs endpoint doesn't exist, return empty logs
            if e.status_code == 404:
                logger.warning("Logs endpoint not available for %s", source)
                return {"logs": [], "error": "Logs endpoint not available"}
            raise

    def wait_for_completion(
        self,
        source: str,
        job_id: str,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        max_attempts: int = MAX_POLL_ATTEMPTS,
    ) -> JobResult:
        """Wait for a scraper job to complete.

        Polls the status endpoint until the job reaches a terminal state
        (completed, failed, or cancelled).

        Args:
            source: Source name ('scjn', 'bjv', 'cas', 'dof')
            job_id: The job ID to monitor
            poll_interval: Seconds between status polls
            max_attempts: Maximum number of poll attempts

        Returns:
            JobResult with final status and metrics

        Raises:
            TimeoutError: If max_attempts is exceeded
        """
        terminal_states = {"completed", "failed", "cancelled", "idle"}
        attempts = 0

        logger.info(
            "Waiting for %s job %s to complete (poll_interval=%ds, max_attempts=%d)",
            source,
            job_id,
            poll_interval,
            max_attempts,
        )

        while attempts < max_attempts:
            status = self.get_status(source)
            current_state = status.get("status", "unknown").lower()

            logger.info(
                "Job %s status: %s (attempt %d/%d)",
                job_id,
                current_state,
                attempts + 1,
                max_attempts,
            )

            if current_state in terminal_states:
                downloaded_count, error_count = self._extract_progress(source, status)
                return JobResult(
                    job_id=job_id,
                    source=source,
                    status=current_state,
                    downloaded_count=downloaded_count,
                    error_count=error_count,
                    embedding_status=self.get_embedding_status(job_id),
                )

            attempts += 1
            time.sleep(poll_interval)

        raise TimeoutError(
            f"Job {job_id} did not complete within {max_attempts * poll_interval} seconds"
        )

    # -------------------------------------------------------------------------
    # Job Control
    # -------------------------------------------------------------------------

    def pause_job(self, source: str) -> dict:
        """Pause a running job.

        Args:
            source: Source name ('scjn', 'bjv', 'cas', 'dof')

        Returns:
            Response from the pause endpoint
        """
        return self._request("POST", f"/api/{source}/pause")

    def resume_job(self, source: str) -> dict:
        """Resume a paused job.

        Args:
            source: Source name ('scjn', 'bjv', 'cas', 'dof')

        Returns:
            Response from the resume endpoint
        """
        return self._request("POST", f"/api/{source}/resume")

    def cancel_job(self, source: str) -> dict:
        """Cancel a running or paused job.

        Args:
            source: Source name ('scjn', 'bjv', 'cas', 'dof')

        Returns:
            Response from the cancel endpoint
        """
        return self._request("POST", f"/api/{source}/cancel")
