#!/usr/bin/env python3
"""
SCJN Live Validation Script

Validates parsers against the real SCJN website with rate-limited requests.
This script tests that our parsers work correctly with actual HTML from the live site.

Usage:
    python -m scripts.validate_scjn_live --mode search
    python -m scripts.validate_scjn_live --mode detail --q-param ABC123
    python -m scripts.validate_scjn_live --mode full --max-docs 5
"""
import argparse
import asyncio
import aiohttp
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum

from src.infrastructure.adapters.scjn_search_parser import (
    parse_search_results,
    extract_pagination_info,
    SearchResultItem,
)
from src.infrastructure.adapters.scjn_document_parser import (
    parse_document_detail,
    parse_reforms,
    DocumentDetailResult,
)


class ValidationMode(Enum):
    """Validation modes."""
    SEARCH = "search"
    DETAIL = "detail"
    FULL = "full"


@dataclass(frozen=True)
class ValidationResult:
    """Result of a single validation check."""
    check_name: str
    passed: bool
    message: str
    details: Optional[Dict[str, Any]] = None


@dataclass
class ValidationReport:
    """Complete validation report."""
    mode: ValidationMode
    started_at: datetime
    completed_at: Optional[datetime] = None
    results: List[ValidationResult] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    @property
    def success_rate(self) -> float:
        if not self.results:
            return 0.0
        return self.passed_count / len(self.results)

    def add_result(self, result: ValidationResult) -> None:
        self.results.append(result)

    def add_error(self, error: str) -> None:
        self.errors.append(error)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode.value,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "passed_count": self.passed_count,
            "failed_count": self.failed_count,
            "success_rate": self.success_rate,
            "results": [
                {
                    "check_name": r.check_name,
                    "passed": r.passed,
                    "message": r.message,
                    "details": r.details,
                }
                for r in self.results
            ],
            "errors": self.errors,
        }


class SCJNLiveValidator:
    """
    Validates SCJN parsers against the live website.

    Uses rate limiting to be respectful to the server.
    """

    BASE_URL = "https://www.scjn.gob.mx/Buscador/Paginas"
    SEARCH_URL = f"{BASE_URL}/Buscar.aspx"
    DETAIL_URL = f"{BASE_URL}/wfOrdenamientoDetalle.aspx"

    def __init__(
        self,
        rate_limit_seconds: float = 2.0,
        timeout_seconds: float = 30.0,
    ):
        self.rate_limit_seconds = rate_limit_seconds
        self.timeout_seconds = timeout_seconds
        self._last_request_time: Optional[float] = None

    async def _rate_limit(self) -> None:
        """Wait for rate limit if needed."""
        if self._last_request_time is not None:
            elapsed = asyncio.get_event_loop().time() - self._last_request_time
            if elapsed < self.rate_limit_seconds:
                await asyncio.sleep(self.rate_limit_seconds - elapsed)
        self._last_request_time = asyncio.get_event_loop().time()

    async def _fetch_html(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: Optional[Dict[str, str]] = None,
    ) -> Tuple[str, int]:
        """Fetch HTML from URL with rate limiting."""
        await self._rate_limit()

        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)

        async with session.get(url, params=params, timeout=timeout) as response:
            html = await response.text()
            return html, response.status

    async def validate_search_page(
        self,
        session: aiohttp.ClientSession,
        page: int = 1,
        category: Optional[str] = None,
    ) -> List[ValidationResult]:
        """Validate search page parsing."""
        results = []

        params = {"pagina": str(page)}
        if category:
            params["categoria"] = category

        try:
            html, status = await self._fetch_html(session, self.SEARCH_URL, params)

            # Check HTTP status
            results.append(ValidationResult(
                check_name="search_http_status",
                passed=status == 200,
                message=f"HTTP status {status}" if status != 200 else "HTTP 200 OK",
            ))

            if status != 200:
                return results

            # Check HTML not empty
            results.append(ValidationResult(
                check_name="search_html_not_empty",
                passed=len(html) > 0,
                message=f"HTML length: {len(html)} chars",
            ))

            # Try to parse
            try:
                documents = parse_search_results(html)
                current_page, total_pages, _ = extract_pagination_info(html)

                # Check documents found
                results.append(ValidationResult(
                    check_name="search_parse_success",
                    passed=True,
                    message=f"Parsed {len(documents)} documents",
                    details={"document_count": len(documents)},
                ))

                # Check pagination info
                results.append(ValidationResult(
                    check_name="search_pagination_info",
                    passed=current_page >= 1 and total_pages >= 1,
                    message=f"Page {current_page} of {total_pages}",
                    details={
                        "current_page": current_page,
                        "total_pages": total_pages,
                    },
                ))

                # Check document structure
                if documents:
                    doc = documents[0]
                    has_q_param = bool(doc.q_param)
                    has_title = bool(doc.title)

                    results.append(ValidationResult(
                        check_name="search_document_structure",
                        passed=has_q_param and has_title,
                        message=f"q_param: {has_q_param}, title: {has_title}",
                        details={
                            "sample_q_param": doc.q_param,
                            "sample_title": doc.title[:50] if doc.title else None,
                        },
                    ))

            except Exception as e:
                results.append(ValidationResult(
                    check_name="search_parse_success",
                    passed=False,
                    message=f"Parse error: {str(e)}",
                ))

        except aiohttp.ClientError as e:
            results.append(ValidationResult(
                check_name="search_http_status",
                passed=False,
                message=f"Connection error: {str(e)}",
            ))

        return results

    async def validate_detail_page(
        self,
        session: aiohttp.ClientSession,
        q_param: str,
    ) -> List[ValidationResult]:
        """Validate document detail page parsing."""
        results = []

        try:
            html, status = await self._fetch_html(
                session,
                self.DETAIL_URL,
                {"q": q_param},
            )

            # Check HTTP status
            results.append(ValidationResult(
                check_name="detail_http_status",
                passed=status == 200,
                message=f"HTTP status {status}" if status != 200 else "HTTP 200 OK",
                details={"q_param": q_param},
            ))

            if status != 200:
                return results

            # Check HTML not empty
            results.append(ValidationResult(
                check_name="detail_html_not_empty",
                passed=len(html) > 0,
                message=f"HTML length: {len(html)} chars",
            ))

            # Try to parse
            try:
                doc = parse_document_detail(html)
                reforms = parse_reforms(html)

                results.append(ValidationResult(
                    check_name="detail_parse_success",
                    passed=True,
                    message=f"Parsed document: {doc.title[:50] if doc.title else 'No title'}",
                    details={
                        "title": doc.title,
                        "category": doc.category,
                        "status": doc.status,
                    },
                ))

                # Check required fields
                has_title = bool(doc.title)
                results.append(ValidationResult(
                    check_name="detail_has_title",
                    passed=has_title,
                    message=f"Title: {'present' if has_title else 'missing'}",
                ))

                # Check reforms if present
                reform_count = len(reforms)
                results.append(ValidationResult(
                    check_name="detail_reforms_parsed",
                    passed=True,  # Reforms are optional
                    message=f"Found {reform_count} reforms",
                    details={"reform_count": reform_count},
                ))

            except Exception as e:
                results.append(ValidationResult(
                    check_name="detail_parse_success",
                    passed=False,
                    message=f"Parse error: {str(e)}",
                ))

        except aiohttp.ClientError as e:
            results.append(ValidationResult(
                check_name="detail_http_status",
                passed=False,
                message=f"Connection error: {str(e)}",
            ))

        return results

    async def run_validation(
        self,
        mode: ValidationMode,
        q_param: Optional[str] = None,
        max_docs: int = 5,
    ) -> ValidationReport:
        """Run validation based on mode."""
        report = ValidationReport(mode=mode, started_at=datetime.now())

        connector = aiohttp.TCPConnector(limit=1)  # Single connection
        async with aiohttp.ClientSession(connector=connector) as session:
            if mode == ValidationMode.SEARCH:
                results = await self.validate_search_page(session)
                for r in results:
                    report.add_result(r)

            elif mode == ValidationMode.DETAIL:
                if not q_param:
                    report.add_error("q_param required for detail mode")
                else:
                    results = await self.validate_detail_page(session, q_param)
                    for r in results:
                        report.add_result(r)

            elif mode == ValidationMode.FULL:
                # First, get search results
                search_results = await self.validate_search_page(session)
                for r in search_results:
                    report.add_result(r)

                # Then validate detail pages for found documents
                try:
                    html, _ = await self._fetch_html(session, self.SEARCH_URL)
                    documents = parse_search_results(html)

                    docs_to_check = documents[:max_docs]
                    for doc in docs_to_check:
                        detail_results = await self.validate_detail_page(
                            session,
                            doc.q_param,
                        )
                        for r in detail_results:
                            report.add_result(r)

                except Exception as e:
                    report.add_error(f"Full validation error: {str(e)}")

        report.completed_at = datetime.now()
        return report


def print_report(report: ValidationReport) -> None:
    """Print validation report to console."""
    print("\n" + "=" * 60)
    print("SCJN LIVE VALIDATION REPORT")
    print("=" * 60)
    print(f"Mode: {report.mode.value}")
    print(f"Started: {report.started_at}")
    print(f"Completed: {report.completed_at}")
    print(f"Duration: {(report.completed_at - report.started_at).total_seconds():.2f}s")
    print("-" * 60)
    print(f"Results: {report.passed_count} passed, {report.failed_count} failed")
    print(f"Success rate: {report.success_rate * 100:.1f}%")
    print("-" * 60)

    for result in report.results:
        status = "[PASS]" if result.passed else "[FAIL]"
        print(f"  {status} {result.check_name}: {result.message}")

    if report.errors:
        print("-" * 60)
        print("ERRORS:")
        for error in report.errors:
            print(f"  - {error}")

    print("=" * 60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Validate SCJN parsers against live website",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["search", "detail", "full"],
        default="search",
        help="Validation mode (default: search)",
    )
    parser.add_argument(
        "--q-param",
        type=str,
        default=None,
        help="Document q_param for detail mode",
    )
    parser.add_argument(
        "--max-docs",
        type=int,
        default=5,
        help="Max documents to check in full mode (default: 5)",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=2.0,
        help="Seconds between requests (default: 2.0)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output file for JSON report",
    )

    args = parser.parse_args()

    mode = ValidationMode(args.mode)

    validator = SCJNLiveValidator(
        rate_limit_seconds=args.rate_limit,
    )

    print(f"Running {mode.value} validation...")

    report = asyncio.run(validator.run_validation(
        mode=mode,
        q_param=args.q_param,
        max_docs=args.max_docs,
    ))

    print_report(report)

    if args.output:
        output_path = Path(args.output)
        output_path.write_text(json.dumps(report.to_dict(), indent=2))
        print(f"\nReport saved to: {output_path}")

    # Exit with error code if any failures
    sys.exit(0 if report.failed_count == 0 else 1)


if __name__ == "__main__":
    main()
