"""
DOF sumario.xml parser — structured RSS feed for daily DOF publications.
Provides section/agency metadata that the HTML index parser cannot.
Feed is ISO-8859-1 encoded.
"""
import re
from typing import List, Dict


def parse_dof_sumario(xml_bytes: bytes) -> List[Dict[str, str]]:
    """
    Parse DOF sumario.xml feed and return structured publication items.

    Args:
        xml_bytes: Raw bytes from https://dof.gob.mx/sumario.xml

    Returns:
        List of dicts with: url, title, section, cod_diario, date
    """
    text = xml_bytes.decode("latin-1", errors="replace")
    items = []

    for match in re.finditer(r"<item>(.*?)</item>", text, re.DOTALL):
        block = match.group(1)

        title_m = re.search(r"<title>(.*?)</title>", block)
        desc_m = re.search(r"<description><!\[CDATA\[(.*?)\]\]></description>", block)
        link_m = re.search(r"<link><!\[CDATA\[(.*?)\]\]></link>", block)
        date_m = re.search(r"<valueDate>(.*?)</valueDate>", block)
        codigo_m = re.search(r"codigo=(\d+)", link_m.group(1) if link_m else "")

        if not codigo_m:
            continue

        section = title_m.group(1).strip() if title_m else None
        description = desc_m.group(1).strip() if desc_m else None
        url = link_m.group(1).strip() if link_m else None
        cod_diario = codigo_m.group(1)
        pub_date = date_m.group(1).strip() if date_m else None

        items.append({
            "url": url or f"https://dof.gob.mx/nota_detalle.php?codigo={cod_diario}",
            "title": description or "Unknown",
            "section": section,
            "cod_diario": cod_diario,
            "date": pub_date,
        })

    return items
