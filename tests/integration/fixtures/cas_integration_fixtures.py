"""
Integration test fixtures for CAS scraper.

Provides mock servers, sample data, and test utilities.
"""
import asyncio
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path
from datetime import datetime
import tempfile


@dataclass
class MockCASResponse:
    """Mock response from CAS website."""
    status_code: int = 200
    html: str = ""
    delay_seconds: float = 0.0


@dataclass
class MockCASServer:
    """
    Mock CAS server for integration testing.

    Simulates the CAS jurisprudence website without actual HTTP requests.
    """
    responses: Dict[str, MockCASResponse] = field(default_factory=dict)
    request_log: List[str] = field(default_factory=list)
    default_search_results: str = ""
    default_award_detail: str = ""

    def __post_init__(self):
        if not self.default_search_results:
            self.default_search_results = MOCK_SEARCH_RESULTS_HTML
        if not self.default_award_detail:
            self.default_award_detail = MOCK_AWARD_DETAIL_HTML

    async def get(self, url: str) -> MockCASResponse:
        """Get mock response for URL."""
        self.request_log.append(url)

        if url in self.responses:
            response = self.responses[url]
            if response.delay_seconds > 0:
                await asyncio.sleep(response.delay_seconds)
            return response

        # Default responses based on URL pattern
        if "search" in url or url.endswith("/"):
            return MockCASResponse(html=self.default_search_results)
        elif "case" in url or "award" in url:
            return MockCASResponse(html=self.default_award_detail)

        return MockCASResponse(status_code=404, html="<html><body>Not Found</body></html>")

    def add_response(self, url: str, html: str, status_code: int = 200) -> None:
        """Add a mock response for a specific URL."""
        self.responses[url] = MockCASResponse(
            status_code=status_code,
            html=html,
        )

    def add_error_response(self, url: str, status_code: int = 500) -> None:
        """Add an error response for a URL."""
        self.responses[url] = MockCASResponse(
            status_code=status_code,
            html=f"<html><body>Error {status_code}</body></html>",
        )

    def add_rate_limit(self, url: str) -> None:
        """Add a rate limit response."""
        self.responses[url] = MockCASResponse(
            status_code=429,
            html="<html><body>Too Many Requests</body></html>",
        )

    def clear_log(self) -> None:
        """Clear request log."""
        self.request_log.clear()


# Sample HTML responses
MOCK_SEARCH_RESULTS_HTML = """
<!DOCTYPE html>
<html>
<head><title>CAS Jurisprudence Search Results</title></head>
<body>
<div class="search-results">
    <div class="case-item" data-case-id="1">
        <span class="case-number">CAS 2024/A/10836</span>
        <a href="/case/10836">Al Nasr Club v. A. Al Daihani</a>
        <span class="date">15 December 2024</span>
        <span class="sport">Football</span>
    </div>
    <div class="case-item" data-case-id="2">
        <span class="case-number">CAS 2024/A/10872</span>
        <a href="/case/10872">WADA v. R. Radhika</a>
        <span class="date">10 December 2024</span>
        <span class="matter">Doping</span>
    </div>
    <div class="case-item" data-case-id="3">
        <span class="case-number">CAS 2023/A/9500</span>
        <a href="/case/9500">FC Example v. Player X</a>
        <span class="date">5 March 2023</span>
        <span class="sport">Football</span>
        <span class="matter">Transfer</span>
    </div>
</div>
<div class="pagination">
    <span class="current">1</span>
    <a href="?page=2">2</a>
    <a href="?page=3">3</a>
    <span class="total-results">Showing 1-20 of 150 results</span>
</div>
</body>
</html>
"""

MOCK_AWARD_DETAIL_HTML = """
<!DOCTYPE html>
<html>
<head><title>CAS 2024/A/10836</title></head>
<body>
<div class="award-detail">
    <h1 class="case-number">CAS 2024/A/10836</h1>
    <h2 class="case-title">Al Nasr Club v. Amash Mohamed Al Daihani et al.</h2>

    <div class="award-date">Award of 15 December 2024</div>

    <div class="parties">
        <div class="appellant">Al Nasr Club (UAE)</div>
        <div class="respondent">Amash Mohamed Al Daihani, FIFA</div>
    </div>

    <div class="panel">
        <div class="arbitrator president">Prof. Ulrich Haas (President)</div>
        <div class="arbitrator">Mr. Efraim Barak</div>
        <div class="arbitrator">Prof. Massimo Coccia</div>
    </div>

    <div class="keywords">Football, Transfer, Contract termination, Compensation</div>

    <div class="summary">
        The Panel decided that the player terminated his contract without just cause
        and ordered compensation to the club.
    </div>

    <div class="facts">
        I. FACTS

        1. Al Nasr Club is a professional football club in the UAE.
        2. The First Respondent is a professional football player from Kuwait.
        3. On 1 January 2022, the parties entered into an employment contract.
        4. The player unilaterally terminated the contract on 15 June 2023.
        5. The club filed a claim with FIFA for breach of contract.
    </div>

    <div class="reasons">
        II. REASONS

        10. The Panel must determine whether just cause existed for termination.
        11. According to established CAS jurisprudence, just cause requires proof
            of serious breach by the other party.
        12. The Panel reviewed the evidence presented by both parties.
        13. The Panel finds that the club did not breach any material obligations.
        14. Therefore, the player terminated without just cause.
    </div>

    <div class="decision">
        III. DECISION

        20. The appeal filed by Al Nasr Club is upheld.
        21. The First Respondent shall pay USD 500,000 to the Appellant.
        22. The costs of arbitration shall be borne by the First Respondent.
        23. All other claims are dismissed.
    </div>

    <a href="/download/CAS_2024_A_10836.pdf" class="pdf-download">Download Full Award (PDF)</a>
</div>
</body>
</html>
"""


@dataclass
class IntegrationTestContext:
    """Context for integration tests."""
    temp_dir: Path
    output_dir: Path
    checkpoint_dir: Path
    log_dir: Path
    mock_server: MockCASServer

    @classmethod
    def create(cls) -> 'IntegrationTestContext':
        """Create a new test context with temp directories."""
        temp_dir = Path(tempfile.mkdtemp(prefix="cas_test_"))

        return cls(
            temp_dir=temp_dir,
            output_dir=temp_dir / "output",
            checkpoint_dir=temp_dir / "checkpoints",
            log_dir=temp_dir / "logs",
            mock_server=MockCASServer(),
        )

    def setup(self) -> None:
        """Create directories."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def cleanup(self) -> None:
        """Remove temp directories."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)


def create_sample_checkpoint(
    session_id: str,
    checkpoint_dir: Path,
    laudos_descubiertos: int = 10,
    laudos_descargados: int = 5,
) -> Path:
    """Create a sample checkpoint file for testing."""
    checkpoint_data = {
        "session_id": session_id,
        "created_at": datetime.utcnow().isoformat(),
        "estado": "pausado",
        "laudos_descubiertos": laudos_descubiertos,
        "laudos_descargados": laudos_descargados,
        "laudos_procesados": laudos_descargados,
        "ultima_pagina": 1,
        "filtros": {
            "max_results": 100,
        },
    }

    checkpoint_file = checkpoint_dir / f"{session_id}.json"
    checkpoint_file.write_text(json.dumps(checkpoint_data, indent=2))

    return checkpoint_file
