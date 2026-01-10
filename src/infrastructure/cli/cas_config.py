"""
CAS CLI configuration and settings.

Centralizes configuration for the CAS scraper CLI,
including default values, paths, and environment variables.
"""
from dataclasses import dataclass
from typing import Optional, Dict, Any
from pathlib import Path
import os


@dataclass(frozen=True)
class CASCliConfig:
    """Configuration for CAS CLI operations."""

    # Output paths
    output_dir: str = "cas_data"
    checkpoint_dir: str = "cas_checkpoints"
    log_dir: str = "cas_logs"

    # Browser settings
    headless: bool = True
    browser_timeout_ms: int = 30000

    # Rate limiting
    rate_limit_delay: float = 3.0  # Seconds between requests
    max_concurrent_downloads: int = 2

    # Retry settings
    max_retries: int = 3
    retry_delay_base: float = 2.0  # Exponential backoff base

    # Defaults
    default_max_results: int = 100
    default_year_from: Optional[int] = None
    default_year_to: Optional[int] = None

    # Logging
    log_level: str = "INFO"
    log_to_file: bool = True

    @classmethod
    def from_env(cls) -> 'CASCliConfig':
        """Create config from environment variables."""
        return cls(
            output_dir=os.getenv("CAS_OUTPUT_DIR", "cas_data"),
            checkpoint_dir=os.getenv("CAS_CHECKPOINT_DIR", "cas_checkpoints"),
            log_dir=os.getenv("CAS_LOG_DIR", "cas_logs"),
            headless=os.getenv("CAS_HEADLESS", "true").lower() == "true",
            browser_timeout_ms=int(os.getenv("CAS_BROWSER_TIMEOUT", "30000")),
            rate_limit_delay=float(os.getenv("CAS_RATE_LIMIT", "3.0")),
            max_concurrent_downloads=int(os.getenv("CAS_MAX_CONCURRENT", "2")),
            max_retries=int(os.getenv("CAS_MAX_RETRIES", "3")),
            log_level=os.getenv("CAS_LOG_LEVEL", "INFO"),
        )

    def ensure_directories(self) -> None:
        """Create required directories if they don't exist."""
        for dir_path in [self.output_dir, self.checkpoint_dir, self.log_dir]:
            Path(dir_path).mkdir(parents=True, exist_ok=True)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "output_dir": self.output_dir,
            "checkpoint_dir": self.checkpoint_dir,
            "log_dir": self.log_dir,
            "headless": self.headless,
            "browser_timeout_ms": self.browser_timeout_ms,
            "rate_limit_delay": self.rate_limit_delay,
            "max_concurrent_downloads": self.max_concurrent_downloads,
            "max_retries": self.max_retries,
            "log_level": self.log_level,
        }


@dataclass(frozen=True)
class SearchFilters:
    """Search filters for CAS discovery."""
    year_from: Optional[int] = None
    year_to: Optional[int] = None
    sport: Optional[str] = None
    matter: Optional[str] = None
    keyword: Optional[str] = None
    procedure_type: Optional[str] = None
    max_results: int = 100

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding None values."""
        result = {}
        if self.year_from:
            result["year_from"] = self.year_from
        if self.year_to:
            result["year_to"] = self.year_to
        if self.sport:
            result["sport"] = self.sport
        if self.matter:
            result["matter"] = self.matter
        if self.keyword:
            result["keyword"] = self.keyword
        if self.procedure_type:
            result["procedure_type"] = self.procedure_type
        result["max_results"] = self.max_results
        return result

    def describe(self) -> str:
        """Human-readable description of filters."""
        parts = []
        if self.year_from or self.year_to:
            year_range = f"{self.year_from or '...'}-{self.year_to or '...'}"
            parts.append(f"años: {year_range}")
        if self.sport:
            parts.append(f"deporte: {self.sport}")
        if self.matter:
            parts.append(f"materia: {self.matter}")
        if self.keyword:
            parts.append(f"palabra clave: {self.keyword}")
        if self.procedure_type:
            parts.append(f"tipo: {self.procedure_type}")
        parts.append(f"máx: {self.max_results}")
        return ", ".join(parts) if parts else "sin filtros"


# Available sports for CLI help (matches CategoriaDeporte enum)
AVAILABLE_SPORTS = [
    "football",
    "athletics",
    "cycling",
    "swimming",
    "basketball",
    "tennis",
    "skiing",
    "other",
]

# Available matter types for CLI help (matches TipoMateria enum)
AVAILABLE_MATTERS = [
    "doping",
    "transfer",
    "eligibility",
    "disciplinary",
    "contractual",
    "governance",
    "other",
]

# Available procedure types
AVAILABLE_PROCEDURES = [
    "appeal",      # A - Apelación
    "ordinary",    # O - Ordinario
    "ad_hoc",      # AHC - Ad Hoc Division
    "anti_doping", # ADD - Anti-Doping Division
]
