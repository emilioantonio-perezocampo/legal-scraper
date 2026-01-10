"""
SCJN Value Objects

Enumerations and value objects for SCJN legislation domain.
These are immutable objects defined by their attributes, not identity.
"""
from enum import Enum


class DocumentCategory(Enum):
    """
    Category of SCJN legal document.

    Represents the type of legal instrument according to Mexican legal system.
    """
    CONSTITUCION = "CONSTITUCION"
    LEY = "LEY"
    LEY_FEDERAL = "LEY_FEDERAL"
    LEY_GENERAL = "LEY_GENERAL"
    LEY_ORGANICA = "LEY_ORGANICA"
    CODIGO = "CODIGO"
    DECRETO = "DECRETO"
    REGLAMENTO = "REGLAMENTO"
    ACUERDO = "ACUERDO"
    TRATADO = "TRATADO"
    CONVENIO = "CONVENIO"


class DocumentScope(Enum):
    """
    Jurisdictional scope of document.

    Defines the territorial/jurisdictional applicability of the legal instrument.
    """
    FEDERAL = "FEDERAL"
    ESTATAL = "ESTATAL"
    CDMX = "CDMX"
    INTERNACIONAL = "INTERNACIONAL"
    EXTRANJERA = "EXTRANJERA"


class DocumentStatus(Enum):
    """
    Current validity status of document.

    Indicates whether the legal instrument is currently in effect.
    """
    VIGENTE = "VIGENTE"
    ABROGADA = "ABROGADA"
    DEROGADA = "DEROGADA"
    SUSTITUIDA = "SUSTITUIDA"
    EXTINTA = "EXTINTA"


class SubjectMatter(Enum):
    """
    Legal subject matter categories.

    Classification of the area of law that the document pertains to.
    """
    ADMINISTRATIVO = "ADMINISTRATIVO"
    CIVIL = "CIVIL"
    CONSTITUCIONAL = "CONSTITUCIONAL"
    ELECTORAL = "ELECTORAL"
    FAMILIAR = "FAMILIAR"
    FISCAL = "FISCAL"
    LABORAL = "LABORAL"
    MERCANTIL = "MERCANTIL"
    PENAL = "PENAL"
    PROCESAL = "PROCESAL"
