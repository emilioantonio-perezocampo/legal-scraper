#!/usr/bin/env python
"""
Live validation script for CAS scraper.

Tests the scraper against the real CAS jurisprudence website
to validate parsing and extraction logic.

Usage:
    python scripts/validate_cas_live.py
    python scripts/validate_cas_live.py --full
    python scripts/validate_cas_live.py --report validation_report.json
"""
import asyncio
import argparse
import json
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))


@dataclass
class ValidationResult:
    """Result of a single validation test."""
    test_name: str
    passed: bool
    duration_ms: float
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class ValidationReport:
    """Complete validation report."""
    timestamp: str
    total_tests: int
    passed_tests: int
    failed_tests: int
    duration_seconds: float
    results: List[ValidationResult] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        if self.total_tests == 0:
            return 0.0
        return (self.passed_tests / self.total_tests) * 100

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "total_tests": self.total_tests,
            "passed_tests": self.passed_tests,
            "failed_tests": self.failed_tests,
            "success_rate": f"{self.success_rate:.1f}%",
            "duration_seconds": self.duration_seconds,
            "results": [asdict(r) for r in self.results],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


class CASLiveValidator:
    """
    Live validation for CAS scraper.

    Performs tests against the real CAS website.
    """

    CAS_BASE_URL = "https://jurisprudence.tas-cas.org/"

    def __init__(self, verbose: bool = False):
        self._verbose = verbose
        self._results: List[ValidationResult] = []

    async def run_all_tests(self, full: bool = False) -> ValidationReport:
        """Run all validation tests."""
        start_time = datetime.utcnow()

        print("=" * 60)
        print("CAS Live Validation")
        print("=" * 60)
        print()

        try:
            from src.infrastructure.adapters.cas_browser_adapter import (
                CASBrowserAdapter,
                BrowserConfig,
                browser_session,
            )

            async with browser_session(BrowserConfig(headless=True)) as browser:
                # Basic tests
                await self._test_homepage_loads(browser)
                await self._test_search_results_parse(browser)
                await self._test_pagination_extraction(browser)

                if full:
                    # Extended tests
                    await self._test_award_detail_parse(browser)
                    await self._test_case_number_extraction(browser)

        except ImportError as e:
            self._add_result(ValidationResult(
                test_name="Browser Import",
                passed=False,
                duration_ms=0,
                message="Failed to import browser adapter",
                error=str(e),
            ))
        except Exception as e:
            self._add_result(ValidationResult(
                test_name="Setup",
                passed=False,
                duration_ms=0,
                message="Failed to initialize browser",
                error=str(e),
            ))

        duration = (datetime.utcnow() - start_time).total_seconds()

        passed = sum(1 for r in self._results if r.passed)
        failed = sum(1 for r in self._results if not r.passed)

        report = ValidationReport(
            timestamp=datetime.utcnow().isoformat(),
            total_tests=len(self._results),
            passed_tests=passed,
            failed_tests=failed,
            duration_seconds=duration,
            results=self._results,
        )

        self._print_summary(report)

        return report

    async def _test_homepage_loads(self, browser) -> None:
        """Test that CAS homepage loads."""
        test_name = "Homepage Loads"
        start = datetime.utcnow()

        try:
            page = await browser.render_page(
                self.CAS_BASE_URL,
                wait_for_network_idle=True,
            )

            duration = (datetime.utcnow() - start).total_seconds() * 1000

            if page.status_code == 200 and len(page.html) > 1000:
                self._add_result(ValidationResult(
                    test_name=test_name,
                    passed=True,
                    duration_ms=duration,
                    message="Homepage loaded successfully",
                    details={
                        "status_code": page.status_code,
                        "html_length": len(page.html),
                        "title": page.title,
                    },
                ))
            else:
                self._add_result(ValidationResult(
                    test_name=test_name,
                    passed=False,
                    duration_ms=duration,
                    message="Homepage loaded but content seems incomplete",
                    details={
                        "status_code": page.status_code,
                        "html_length": len(page.html),
                    },
                ))

        except Exception as e:
            duration = (datetime.utcnow() - start).total_seconds() * 1000
            self._add_result(ValidationResult(
                test_name=test_name,
                passed=False,
                duration_ms=duration,
                message="Failed to load homepage",
                error=str(e),
            ))

    async def _test_search_results_parse(self, browser) -> None:
        """Test parsing of search results."""
        test_name = "Search Results Parse"
        start = datetime.utcnow()

        try:
            from src.infrastructure.adapters.cas_search_parser import parse_search_results

            page = await browser.render_page(
                self.CAS_BASE_URL,
                wait_for_network_idle=True,
            )

            results = parse_search_results(page.html)
            duration = (datetime.utcnow() - start).total_seconds() * 1000

            if len(results) > 0:
                # Validate first result has required fields
                first = results[0]
                has_case_number = bool(first.numero_caso)
                has_url = bool(first.url_detalle)

                self._add_result(ValidationResult(
                    test_name=test_name,
                    passed=has_case_number and has_url,
                    duration_ms=duration,
                    message=f"Parsed {len(results)} search results",
                    details={
                        "result_count": len(results),
                        "first_case": first.numero_caso,
                        "has_url": has_url,
                    },
                ))
            else:
                self._add_result(ValidationResult(
                    test_name=test_name,
                    passed=False,
                    duration_ms=duration,
                    message="No search results found",
                ))

        except Exception as e:
            duration = (datetime.utcnow() - start).total_seconds() * 1000
            self._add_result(ValidationResult(
                test_name=test_name,
                passed=False,
                duration_ms=duration,
                message="Failed to parse search results",
                error=str(e),
            ))

    async def _test_pagination_extraction(self, browser) -> None:
        """Test pagination info extraction."""
        test_name = "Pagination Extraction"
        start = datetime.utcnow()

        try:
            from src.infrastructure.adapters.cas_search_parser import extract_pagination_info

            page = await browser.render_page(
                self.CAS_BASE_URL,
                wait_for_network_idle=True,
            )

            pagination = extract_pagination_info(page.html)
            duration = (datetime.utcnow() - start).total_seconds() * 1000

            self._add_result(ValidationResult(
                test_name=test_name,
                passed=pagination.total_paginas >= 1,
                duration_ms=duration,
                message=f"Extracted pagination: page {pagination.pagina_actual}/{pagination.total_paginas}",
                details={
                    "current_page": pagination.pagina_actual,
                    "total_pages": pagination.total_paginas,
                    "total_results": pagination.total_resultados,
                },
            ))

        except Exception as e:
            duration = (datetime.utcnow() - start).total_seconds() * 1000
            self._add_result(ValidationResult(
                test_name=test_name,
                passed=False,
                duration_ms=duration,
                message="Failed to extract pagination",
                error=str(e),
            ))

    async def _test_award_detail_parse(self, browser) -> None:
        """Test parsing of award detail page."""
        test_name = "Award Detail Parse"
        start = datetime.utcnow()

        try:
            from src.infrastructure.adapters.cas_search_parser import parse_search_results
            from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle

            # First get a search result to find an award URL
            search_page = await browser.render_page(
                self.CAS_BASE_URL,
                wait_for_network_idle=True,
            )
            results = parse_search_results(search_page.html)

            if not results:
                self._add_result(ValidationResult(
                    test_name=test_name,
                    passed=False,
                    duration_ms=0,
                    message="No search results to test detail page",
                ))
                return

            # Fetch first award detail
            first_url = results[0].url_detalle
            if not first_url.startswith("http"):
                first_url = f"https://jurisprudence.tas-cas.org{first_url}"

            detail_page = await browser.render_page(
                first_url,
                wait_for_network_idle=True,
            )

            laudo = parse_laudo_detalle(detail_page.html, first_url)
            duration = (datetime.utcnow() - start).total_seconds() * 1000

            self._add_result(ValidationResult(
                test_name=test_name,
                passed=bool(laudo.numero_caso),
                duration_ms=duration,
                message=f"Parsed award: {laudo.numero_caso.formato_completo if laudo.numero_caso else 'N/A'}",
                details={
                    "case_number": laudo.numero_caso.formato_completo if laudo.numero_caso else None,
                    "title": laudo.titulo[:50] if laudo.titulo else None,
                    "has_parties": len(laudo.partes) > 0,
                    "has_arbitrators": len(laudo.arbitros) > 0,
                },
            ))

        except Exception as e:
            duration = (datetime.utcnow() - start).total_seconds() * 1000
            self._add_result(ValidationResult(
                test_name=test_name,
                passed=False,
                duration_ms=duration,
                message="Failed to parse award detail",
                error=str(e),
            ))

    async def _test_case_number_extraction(self, browser) -> None:
        """Test case number format extraction."""
        test_name = "Case Number Extraction"
        start = datetime.utcnow()

        try:
            from src.infrastructure.adapters.cas_search_parser import parse_search_results
            import re

            page = await browser.render_page(
                self.CAS_BASE_URL,
                wait_for_network_idle=True,
            )
            results = parse_search_results(page.html)

            duration = (datetime.utcnow() - start).total_seconds() * 1000

            # Check case number format
            valid_formats = 0
            for result in results[:10]:
                if result.numero_caso:
                    # Should match CAS YYYY/A/XXXXX or TAS YYYY/A/XXXXX
                    if re.match(r'(CAS|TAS)\s*\d{4}/[A-Z]+/\d+', result.numero_caso, re.IGNORECASE):
                        valid_formats += 1

            self._add_result(ValidationResult(
                test_name=test_name,
                passed=valid_formats > 0,
                duration_ms=duration,
                message=f"{valid_formats}/{min(10, len(results))} case numbers have valid format",
                details={
                    "valid_formats": valid_formats,
                    "sample_cases": [r.numero_caso for r in results[:5]],
                },
            ))

        except Exception as e:
            duration = (datetime.utcnow() - start).total_seconds() * 1000
            self._add_result(ValidationResult(
                test_name=test_name,
                passed=False,
                duration_ms=duration,
                error=str(e),
            ))

    def _add_result(self, result: ValidationResult) -> None:
        """Add a validation result."""
        self._results.append(result)

        status = "PASS" if result.passed else "FAIL"
        print(f"[{status}] {result.test_name}: {result.message}")

        if self._verbose and result.details:
            for key, value in result.details.items():
                print(f"    {key}: {value}")

        if result.error:
            print(f"    Error: {result.error}")

    def _print_summary(self, report: ValidationReport) -> None:
        """Print validation summary."""
        print()
        print("=" * 60)
        print("Validation Summary")
        print("=" * 60)
        print(f"Total Tests: {report.total_tests}")
        print(f"Passed: {report.passed_tests}")
        print(f"Failed: {report.failed_tests}")
        print(f"Success Rate: {report.success_rate:.1f}%")
        print(f"Duration: {report.duration_seconds:.2f}s")
        print("=" * 60)


async def main_async(args: argparse.Namespace) -> int:
    """Async main entry point."""
    validator = CASLiveValidator(verbose=args.verbose)
    report = await validator.run_all_tests(full=args.full)

    # Save report if requested
    if args.report:
        report_path = Path(args.report)
        report_path.write_text(report.to_json())
        print(f"\nReport saved to: {report_path}")

    return 0 if report.failed_tests == 0 else 1


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Live validation for CAS scraper",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full validation suite including detail page tests",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output with details",
    )
    parser.add_argument(
        "--report",
        type=str,
        help="Path to save JSON validation report",
    )

    args = parser.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
