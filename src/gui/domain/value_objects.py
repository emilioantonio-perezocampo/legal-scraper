"""
GUI Domain Value Objects

Value objects are immutable and defined by their attributes.
They have no identity beyond their values.
"""
from enum import Enum
from typing import NamedTuple


class TargetSource(Enum):
    """
    Represents a legal document source that can be scraped.
    Each source has metadata about its location and display name.
    """
    DOF = "dof"
    SCJN = "scjn"
    BJV = "bjv"
    CAS = "cas"  # Court of Arbitration for Sport

    @property
    def display_name(self) -> str:
        """Human-readable name for the source."""
        names = {
            "dof": "Diario Oficial de la Federación",
            "scjn": "SCJN - Legislación Federal/Estatal",
            "bjv": "BJV - Biblioteca Jurídica Virtual UNAM",
            "cas": "CAS/TAS - Tribunal Arbitral du Sport",
        }
        return names.get(self.value, self.value)

    @property
    def base_url(self) -> str:
        """Base URL for the source."""
        urls = {
            "dof": "https://dof.gob.mx/",
            "scjn": "https://www.scjn.gob.mx/",
            "bjv": "https://biblio.juridicas.unam.mx/bjv",
            "cas": "https://jurisprudence.tas-cas.org/",
        }
        return urls.get(self.value, "")

    @property
    def description(self) -> str:
        """Detailed description of the source."""
        descriptions = {
            "dof": "Diario Oficial de la Federación de México",
            "scjn": "Suprema Corte de Justicia de la Nación - Legislación",
            "bjv": "Biblioteca Jurídica Virtual UNAM",
            "cas": "Court of Arbitration for Sport / Tribunal Arbitral du Sport",
        }
        return descriptions.get(self.value, "")


class OutputFormat(Enum):
    """
    Supported output formats for scraped data.
    """
    JSON = "json"

    @property
    def extension(self) -> str:
        """File extension for this format."""
        extensions = {
            "json": ".json",
        }
        return extensions.get(self.value, "")


class ScraperMode(Enum):
    """
    Operating modes for the scraper.
    """
    TODAY = "today"           # Scrape only today's documents
    SINGLE_DATE = "single"    # Scrape a specific single date
    DATE_RANGE = "range"      # Scrape a range of dates
    HISTORICAL = "historical" # Full historical backfill
