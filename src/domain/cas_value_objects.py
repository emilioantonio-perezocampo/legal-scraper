"""
CAS Domain Value Objects

Value objects for the Court of Arbitration for Sport (CAS/TAS) scraper.
All value objects are immutable (frozen dataclasses) with no identity
beyond their values.

Following DDD patterns with Spanish terminology.
"""
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Optional


class TipoProcedimiento(Enum):
    """
    Type of CAS/TAS arbitration procedure.

    Values correspond to CAS classification system.
    """
    ARBITRAJE_ORDINARIO = "ordinary"
    ARBITRAJE_APELACION = "appeal"
    ARBITRAJE_ANTIDOPAJE = "anti-doping"
    MEDIACION = "mediation"
    OPINION_CONSULTIVA = "advisory"


class CategoriaDeporte(Enum):
    """
    Sport category for CAS cases.

    Common sports appearing in CAS jurisprudence.
    """
    FUTBOL = "football"
    ATLETISMO = "athletics"
    CICLISMO = "cycling"
    NATACION = "swimming"
    BALONCESTO = "basketball"
    TENIS = "tennis"
    ESQUI = "skiing"
    OTRO = "other"


class TipoMateria(Enum):
    """
    Subject matter type for CAS cases.

    Classification of legal issues addressed.
    """
    DOPAJE = "doping"
    TRANSFERENCIA = "transfer"
    ELEGIBILIDAD = "eligibility"
    DISCIPLINA = "disciplinary"
    CONTRACTUAL = "contractual"
    GOBERNANZA = "governance"
    OTRO = "other"


class IdiomaLaudo(Enum):
    """
    Language of the arbitral award.

    Official CAS languages.
    """
    INGLES = "en"
    FRANCES = "fr"
    ESPANOL = "es"


class EstadoLaudo(Enum):
    """
    Publication status of the award.
    """
    PUBLICADO = "published"
    PENDIENTE = "pending"
    CONFIDENCIAL = "confidential"


@dataclass(frozen=True)
class NumeroCaso:
    """
    CAS case number value object.

    Format: CAS YYYY/A/XXXXX or TAS YYYY/A/XXXXX

    Immutable identifier for a CAS/TAS case.
    """
    valor: str


@dataclass(frozen=True)
class URLLaudo:
    """
    URL to the arbitral award document.

    Points to the PDF or HTML version on jurisprudence.tas-cas.org.
    """
    valor: str


@dataclass(frozen=True)
class FechaLaudo:
    """
    Date of the arbitral award.

    Represents when the award was rendered.
    """
    valor: date

    @property
    def anio(self) -> int:
        """Return the year of the award."""
        return self.valor.year
