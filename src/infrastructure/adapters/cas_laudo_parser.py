"""
Parser for CAS award detail pages.

Extracts full metadata, parties, panel, and content sections
from rendered award pages.
"""
from typing import Tuple, Optional, List
from bs4 import BeautifulSoup
from datetime import datetime, date
from uuid import uuid4
import re

from src.domain.cas_entities import (
    LaudoArbitral,
    Parte,
    Arbitro,
    Federacion,
    TipoParte,
)
from src.domain.cas_value_objects import (
    NumeroCaso,
    FechaLaudo,
    TipoProcedimiento,
    CategoriaDeporte,
    TipoMateria,
    IdiomaLaudo,
    EstadoLaudo,
)
from src.infrastructure.adapters.cas_errors import ParseError


def parse_laudo_detalle(html: str, url: str) -> LaudoArbitral:
    """
    Parse CAS award detail page.

    Args:
        html: Rendered HTML from browser adapter
        url: Original URL for reference

    Returns:
        LaudoArbitral entity with full metadata

    Raises:
        ParseError: If required fields cannot be extracted
    """
    if not html or not html.strip():
        raise ParseError("HTML vacío recibido")

    soup = BeautifulSoup(html, 'html.parser')
    full_text = soup.get_text(separator="\n", strip=True)

    # Extract case number (required)
    numero_caso = _extract_numero_caso(soup, full_text)
    if not numero_caso:
        raise ParseError(f"No se encontró número de caso en: {url}")

    laudo_id = str(uuid4())

    # Extract title
    titulo = _extract_titulo(soup, full_text) or numero_caso.valor

    # Extract date
    fecha_laudo = _extract_fecha_laudo(soup, full_text)

    # Extract parties
    partes = _extract_partes(soup, full_text, titulo)

    # Extract arbitrators
    arbitros = _extract_arbitros(soup, full_text)

    # Extract federations from parties
    federaciones = _extract_federaciones(partes)

    # Classify sport
    deporte = _detect_deporte(full_text, partes)

    # Classify matter
    materia = _detect_materia(full_text, titulo)

    # Detect language
    idioma = _detect_idioma(full_text)

    # Extract summary
    resumen = _extract_resumen(soup)

    # Extract keywords
    palabras_clave = _extract_palabras_clave(soup, full_text)

    return LaudoArbitral(
        id=laudo_id,
        numero_caso=numero_caso,
        fecha=fecha_laudo or FechaLaudo(valor=date.today()),
        titulo=titulo,
        tipo_procedimiento=_detect_tipo_procedimiento(numero_caso.valor),
        categoria_deporte=deporte,
        materia=materia,
        partes=partes,
        arbitros=arbitros,
        federaciones=federaciones,
        idioma=idioma,
        resumen=resumen,
        estado=EstadoLaudo.PUBLICADO,
        palabras_clave=palabras_clave,
    )


def _extract_numero_caso(soup: BeautifulSoup, text: str) -> Optional[NumeroCaso]:
    """Extract and parse case number."""
    # Pattern for CAS/TAS case numbers
    pattern = re.compile(r'(CAS|TAS)\s*(\d{4})/([A-Z]+)/(\d+)', re.IGNORECASE)

    # Try structured elements first
    selectors = ['.case-number', '.numero-caso', 'h1', 'h2', '.title', '[data-case]']

    for selector in selectors:
        elem = soup.select_one(selector)
        if elem:
            match = pattern.search(elem.get_text())
            if match:
                valor = f"{match.group(1).upper()} {match.group(2)}/{match.group(3).upper()}/{match.group(4)}"
                return NumeroCaso(valor=valor)

    # Fall back to full text search
    match = pattern.search(text)
    if match:
        valor = f"{match.group(1).upper()} {match.group(2)}/{match.group(3).upper()}/{match.group(4)}"
        return NumeroCaso(valor=valor)

    return None


def _extract_titulo(soup: BeautifulSoup, text: str) -> Optional[str]:
    """Extract award title (usually parties)."""
    # Try structured elements
    selectors = ['.case-title', '.parties', 'h1.title', 'h2.parties']

    for selector in selectors:
        elem = soup.select_one(selector)
        if elem:
            titulo = elem.get_text(strip=True)
            if titulo and len(titulo) > 5:
                return titulo[:300]

    # Look for "v." pattern in text
    vs_pattern = re.compile(r'([A-Z][^v]{2,50})\s+v\.?\s+([A-Z][^v]{2,50})', re.MULTILINE)
    match = vs_pattern.search(text[:2000])
    if match:
        return f"{match.group(1).strip()} v. {match.group(2).strip()}"[:300]

    return None


def _extract_fecha_laudo(soup: BeautifulSoup, text: str) -> Optional[FechaLaudo]:
    """Extract award date."""
    # Try structured elements
    date_elem = soup.select_one('.award-date, .date, [data-date]')
    if date_elem:
        date_text = date_elem.get_text(strip=True)
        parsed = _parse_date_text(date_text)
        if parsed:
            return FechaLaudo(valor=parsed)

    # Look for date patterns in text
    date_patterns = [
        r'Award\s+of\s+(\d{1,2}\s+\w+\s+\d{4})',
        r'dated\s+(\d{1,2}\s+\w+\s+\d{4})',
        r'rendered\s+on\s+(\d{1,2}\s+\w+\s+\d{4})',
        r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
    ]

    for pattern in date_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            parsed = _parse_date_text(match.group(1))
            if parsed:
                return FechaLaudo(valor=parsed)

    return None


def _parse_date_text(text: str) -> Optional[date]:
    """Parse date from text."""
    months = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12
    }

    pattern = re.compile(r'(\d{1,2})\s+(\w+)\s+(\d{4})', re.IGNORECASE)
    match = pattern.search(text)

    if match:
        day = int(match.group(1))
        month_name = match.group(2).lower()
        year = int(match.group(3))

        if month_name in months:
            try:
                return date(year, months[month_name], day)
            except ValueError:
                pass

    return None


def _extract_partes(soup: BeautifulSoup, text: str, titulo: str) -> Tuple[Parte, ...]:
    """Extract parties from award."""
    partes: List[Parte] = []

    # Try structured elements
    appellant_elem = soup.select_one('.appellant, .apelante, [data-party="appellant"]')
    if appellant_elem:
        nombre = appellant_elem.get_text(strip=True)
        if nombre:
            partes.append(Parte(
                nombre=nombre[:200],
                tipo=_detect_tipo_parte(nombre),
            ))

    respondent_elem = soup.select_one('.respondent, .demandado, [data-party="respondent"]')
    if respondent_elem:
        nombre = respondent_elem.get_text(strip=True)
        if nombre:
            partes.append(Parte(
                nombre=nombre[:200],
                tipo=_detect_tipo_parte(nombre),
            ))

    # If no structured elements, try to parse from title
    if not partes and titulo:
        vs_match = re.match(r'(.+?)\s+v\.?\s+(.+)', titulo, re.IGNORECASE)
        if vs_match:
            appellant = vs_match.group(1).strip()
            respondent = vs_match.group(2).strip()

            partes.append(Parte(
                nombre=appellant[:200],
                tipo=_detect_tipo_parte(appellant),
            ))
            partes.append(Parte(
                nombre=respondent[:200],
                tipo=_detect_tipo_parte(respondent),
            ))

    return tuple(partes)


def _detect_tipo_parte(nombre: str) -> TipoParte:
    """Detect party type from name.

    Maps detected entity types to CAS procedural roles:
    - Federations/Clubs typically appear as APELADO (respondent)
    - Athletes/individuals typically appear as APELANTE (appellant)
    """
    nombre_lower = nombre.lower()

    # Federation indicators - typically respondents
    if any(x in nombre_lower for x in ['fifa', 'wada', 'ioc', 'uci', 'iaaf', 'fiba', 'itf', 'fei', 'fide', 'fina']):
        return TipoParte.APELADO
    if any(x in nombre_lower for x in ['federation', 'association', 'union', 'committee']):
        return TipoParte.APELADO

    # Club indicators - typically respondents
    if any(x in nombre_lower for x in ['fc', 'club', 'united', 'city', 'real', 'sporting']):
        return TipoParte.APELADO

    # Default to appellant (athletes/individuals usually appeal)
    return TipoParte.APELANTE


def _extract_arbitros(soup: BeautifulSoup, text: str) -> Tuple[Arbitro, ...]:
    """Extract panel arbitrators."""
    arbitros: List[Arbitro] = []

    # Try structured elements
    panel_elems = soup.select('.arbitrator, .arbitro, .panel-member, [data-arbitrator]')

    for elem in panel_elems:
        nombre = elem.get_text(strip=True)
        if nombre and len(nombre) > 3:
            # Check if president
            is_president = 'president' in nombre.lower() or 'president' in str(elem.get('class', [])).lower()
            clean_name = re.sub(r'\s*\(President\)\s*', '', nombre, flags=re.IGNORECASE).strip()

            arbitros.append(Arbitro(
                nombre=clean_name[:150],
                nacionalidad="",
                rol="President" if is_president else None,
            ))

    # If no structured elements, look for panel section
    if not arbitros:
        panel_patterns = [
            r'(?:Panel|Arbitrators?|Tribunal)[\s:]+([^\n]+(?:\n[^\n]+){0,5})',
            r'(?:President|Chair)[\s:]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
        ]

        for pattern in panel_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                panel_text = match.group(1)
                # Extract names (capitalized words)
                names = re.findall(r'(?:Mr\.?|Ms\.?|Dr\.?|Prof\.?)?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)', panel_text)
                for name in names[:5]:  # Limit to 5 arbitrators
                    if len(name) > 3:
                        arbitros.append(Arbitro(nombre=name.strip()[:150], nacionalidad=""))
                break

    return tuple(arbitros)


def _extract_federaciones(partes: Tuple[Parte, ...]) -> Tuple[Federacion, ...]:
    """Extract federations from parties."""
    federaciones: List[Federacion] = []

    known_feds = {
        'fifa': ('FIFA', 'Fédération Internationale de Football Association', CategoriaDeporte.FUTBOL),
        'wada': ('WADA', 'World Anti-Doping Agency', None),
        'ioc': ('IOC', 'International Olympic Committee', None),
        'uci': ('UCI', 'Union Cycliste Internationale', CategoriaDeporte.CICLISMO),
        'iaaf': ('WA', 'World Athletics', CategoriaDeporte.ATLETISMO),
        'world athletics': ('WA', 'World Athletics', CategoriaDeporte.ATLETISMO),
        'fiba': ('FIBA', 'International Basketball Federation', CategoriaDeporte.BALONCESTO),
        'itf': ('ITF', 'International Tennis Federation', CategoriaDeporte.TENIS),
        'fei': ('FEI', 'Fédération Equestre Internationale', None),
        'fide': ('FIDE', 'World Chess Federation', None),
        'fina': ('FINA', 'World Aquatics', CategoriaDeporte.NATACION),
    }

    seen = set()

    for parte in partes:
        nombre_lower = parte.nombre.lower()
        for key, (siglas, nombre_completo, deporte) in known_feds.items():
            if key in nombre_lower and siglas not in seen:
                seen.add(siglas)
                federaciones.append(Federacion(
                    nombre=nombre_completo,
                    acronimo=siglas,
                    deporte=deporte,
                ))

    return tuple(federaciones)


def _detect_deporte(text: str, partes: Tuple[Parte, ...]) -> Optional[CategoriaDeporte]:
    """Detect sport from content."""
    combined = text.lower()
    for parte in partes:
        combined += " " + parte.nombre.lower()

    sport_keywords = {
        CategoriaDeporte.FUTBOL: ['football', 'soccer', 'fifa'],
        CategoriaDeporte.ATLETISMO: ['athletics', 'iaaf', 'world athletics', 'track', 'marathon'],
        CategoriaDeporte.CICLISMO: ['cycling', 'cyclist', 'uci', 'tour'],
        CategoriaDeporte.NATACION: ['swimming', 'swimmer', 'fina', 'aquatics'],
        CategoriaDeporte.BALONCESTO: ['basketball', 'fiba'],
        CategoriaDeporte.TENIS: ['tennis', 'itf', 'atp', 'wta'],
    }

    for deporte, keywords in sport_keywords.items():
        if any(kw in combined for kw in keywords):
            return deporte

    return None


def _detect_materia(text: str, titulo: str) -> Optional[TipoMateria]:
    """Detect subject matter."""
    combined = (text + " " + titulo).lower()

    matter_keywords = {
        TipoMateria.DOPAJE: ['doping', 'anti-doping', 'wada', 'prohibited substance', 'adrv'],
        TipoMateria.TRANSFERENCIA: ['transfer', 'training compensation', 'solidarity'],
        TipoMateria.CONTRACTUAL: ['contract', 'employment', 'salary', 'termination'],
        TipoMateria.ELEGIBILIDAD: ['eligibility', 'nationality', 'registration'],
        TipoMateria.DISCIPLINA: ['disciplinary', 'sanction', 'suspension', 'ban'],
    }

    for materia, keywords in matter_keywords.items():
        if any(kw in combined for kw in keywords):
            return materia

    return None


def _detect_idioma(text: str) -> IdiomaLaudo:
    """Detect award language."""
    # Sample first 1000 chars
    sample = text[:1000].lower()

    # French indicators
    french_words = ['le', 'la', 'les', 'de', 'du', 'des', 'que', 'qui', 'dans', 'pour', 'est', 'sont']
    french_count = sum(1 for word in french_words if f' {word} ' in f' {sample} ')

    # English indicators
    english_words = ['the', 'and', 'of', 'that', 'which', 'this', 'was', 'were', 'has', 'have']
    english_count = sum(1 for word in english_words if f' {word} ' in f' {sample} ')

    if french_count > english_count + 3:
        return IdiomaLaudo.FRANCES

    return IdiomaLaudo.INGLES


def _detect_tipo_procedimiento(numero_caso: str) -> Optional[TipoProcedimiento]:
    """Detect procedure type from case number."""
    if '/A/' in numero_caso:
        return TipoProcedimiento.ARBITRAJE_APELACION
    if '/O/' in numero_caso:
        return TipoProcedimiento.ARBITRAJE_ORDINARIO
    return None


def _extract_resumen(soup: BeautifulSoup) -> Optional[str]:
    """Extract award summary/abstract."""
    selectors = ['.summary', '.abstract', '.resumen', '[data-section="summary"]']

    for selector in selectors:
        elem = soup.select_one(selector)
        if elem:
            text = elem.get_text(strip=True)
            if len(text) > 20:
                return text[:2000]

    return None


def _extract_palabras_clave(soup: BeautifulSoup, text: str) -> Tuple[str, ...]:
    """Extract keywords."""
    keywords: List[str] = []

    # Try structured elements
    kw_elem = soup.select_one('.keywords, .tags, [data-keywords]')
    if kw_elem:
        text_kw = kw_elem.get_text(strip=True)
        keywords.extend([k.strip() for k in re.split(r'[,;]', text_kw) if k.strip()])

    return tuple(keywords[:20])


def _detect_tipo_decision(texto: str) -> str:
    """Detect decision type from operative text."""
    texto_lower = texto.lower()

    # Check for partial first (more specific)
    if 'partially' in texto_lower:
        return "parcial"

    if any(x in texto_lower for x in ['dismissed', 'rejected', 'inadmissible']):
        return "desestimatoria"
    if any(x in texto_lower for x in ['upheld', 'granted', 'allowed', 'successful']):
        return "estimatoria"

    return "otro"


def _extract_casos_citados(text: str) -> Tuple[str, ...]:
    """Extract cited CAS cases."""
    pattern = re.compile(r'(CAS|TAS)\s*\d{4}/[A-Z]+/\d+', re.IGNORECASE)

    seen = set()
    result = []

    for match in pattern.finditer(text):
        caso = match.group(0).upper()
        caso = re.sub(r'\s+', ' ', caso)
        if caso not in seen:
            seen.add(caso)
            result.append(caso)

    return tuple(result[:30])  # Limit to 30
