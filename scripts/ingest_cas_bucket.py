#!/usr/bin/env python3
"""
CAS Bucket Ingestion Script

Ingests CAS arbitration award PDFs and JSON metadata from the
'ley-deportiva' Supabase Storage bucket into the scraper_documents
and cas_laudos tables.

Usage:
    python scripts/ingest_cas_bucket.py [--register-docs] [--dry-run]

Options:
    --register-docs  Also register documents in document_registry for deep processing
    --dry-run        Print what would be done without inserting
"""
from __future__ import annotations

import json
import os
import re
import sys
import uuid
from datetime import date, datetime
from typing import Any, Optional

from supabase import create_client, Client


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BUCKET_NAME = "ley-deportiva"
SUPABASE_URL = os.environ.get(
    "SUPABASE_URL",
    os.environ.get(
        "API_EXTERNAL_URL",
        "https://dev.ragsystem.legaltracking.generalanalyticsolutions.com",
    ),
)
SERVICE_ROLE_KEY = os.environ.get(
    "SUPABASE_SERVICE_ROLE_KEY",
    os.environ.get("SERVICE_ROLE_KEY", ""),
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_supabase_client() -> Client:
    """Create Supabase client with service role key."""
    if not SERVICE_ROLE_KEY:
        print("ERROR: SUPABASE_SERVICE_ROLE_KEY or SERVICE_ROLE_KEY env var required")
        sys.exit(1)
    return create_client(SUPABASE_URL, SERVICE_ROLE_KEY)


def download_json_from_bucket(client: Client, path: str) -> Any:
    """Download and parse a JSON file from the bucket."""
    data = client.storage.from_(BUCKET_NAME).download(path)
    return json.loads(data.decode("utf-8"))


def parse_case_number(case_number: str) -> dict[str, Optional[str]]:
    """
    Parse a CAS case number into components.

    Examples:
        "CAS 2022/A/8871"      → year=2022, proc=A,   num=8871
        "CAS 2023/ADD/59"      → year=2023, proc=ADD, num=59
        "CAS 2023/O/9401"      → year=2023, proc=O,   num=9401
        "CAS 2022/A/8865-8868" → year=2022, proc=A,   num=8865-8868
        "CAS 2022/A/9328 & 9329" → year=2022, proc=A, num=9328 & 9329
    """
    m = re.match(r"CAS\s+(\d{4})/(A|ADD|O)/(.+)", case_number.strip())
    if not m:
        return {"year": None, "procedure": None, "number": None}
    return {
        "year": m.group(1),
        "procedure": m.group(2),
        "number": m.group(3).strip(),
    }


def procedure_type_from_code(code: Optional[str]) -> Optional[str]:
    """Map CAS procedure code to domain enum value."""
    mapping = {
        "A": "appeal",
        "ADD": "anti-doping",
        "O": "ordinary",
    }
    return mapping.get(code) if code else None


def clean_party_name(raw: str) -> str:
    """
    Extract party name from CAS JSON format.

    JSON has: "Name\nREPRESENTATIVE(S):lawyer names..."
    We want just the party name.
    """
    # Split on REPRESENTATIVE(S): marker
    parts = re.split(r"\nREPRESENTATIVE\(S\):", raw, maxsplit=1)
    name = parts[0].strip()
    # Some names have multiple parties separated by newlines
    # Take only the first line as the primary party name
    lines = [l.strip() for l in name.split("\n") if l.strip()]
    return lines[0] if lines else name


def extract_numbers_from_pdf_name(pdf_name: str) -> list[str]:
    """
    Extract case number(s) from a PDF filename.

    Examples:
        "8871.pdf"                    → ["8871"]
        "378-O.pdf"                   → ["378"]
        "8865208866208867208868.pdf"  → ["8865", "8866", "8867", "8868"]
        "ADD2059.pdf"                 → ["59"]  (ADD prefix)
        "9328209329.pdf"              → ["9328", "9329"]
        "8915208918208919208920.pdf"  → ["8915", "8918", "8919", "8920"]
    """
    name = pdf_name.replace(".pdf", "")

    # Handle ADD prefix
    if name.startswith("ADD"):
        # ADD2059 → 59 (remove ADD prefix, the '20' is part of the number encoding)
        # But ADD59 → 59
        inner = name[3:]  # Remove "ADD"
        # CAS ADD numbers are low (typically 2-3 digits)
        # ADD2059 means case number 59, the "20" is encoding artifact
        # Look for the actual number by checking the JSON metadata
        return [inner]

    # Handle -O suffix (ordinary procedure)
    name = re.sub(r"-O$", "", name)

    # Check if it's a concatenated multi-case number
    # Pattern: 4-5 digit numbers joined with "20" separator
    # e.g., 8865208866208867208868 → 8865, 8866, 8867, 8868
    # Heuristic: if the name is > 5 chars, try splitting on "20" boundaries
    if len(name) > 5 and name.isdigit():
        # Try to split: look for 4-5 digit numbers separated by "20"
        # The separator "20" appears between numbers
        parts = re.split(r"(?<=\d{4})20(?=\d{4})", name)
        if len(parts) > 1:
            return parts

    return [name]


def match_pdf_to_case_number(
    pdf_name: str,
    all_cases_by_number: dict[str, dict],
) -> Optional[dict]:
    """
    Try to match a PDF filename to a case from all_cases.json metadata.

    Returns the matching case metadata dict, or None if no match.
    """
    nums = extract_numbers_from_pdf_name(pdf_name)
    base_name = pdf_name.replace(".pdf", "")

    # Strategy 1: Check if any extracted number matches a case's last number segment
    for case_number, case_data in all_cases_by_number.items():
        parsed = parse_case_number(case_number)
        if not parsed["number"]:
            continue

        # For multi-case entries like "8865-8868" or "9328 & 9329"
        # extract all individual numbers
        case_nums = re.findall(r"\d+", parsed["number"])

        # Check if our PDF's first number matches the first case number
        if nums and case_nums and nums[0] == case_nums[0]:
            return case_data

    # Strategy 2: For ADD cases, match ADD prefix
    if base_name.startswith("ADD"):
        add_num = base_name[3:]
        # Try removing leading "20" if present (encoding artifact)
        for suffix in [add_num, add_num.lstrip("20")]:
            for case_number, case_data in all_cases_by_number.items():
                if f"ADD/{suffix}" in case_number:
                    return case_data

    # Strategy 3: For O (ordinary) cases
    if "-O" in pdf_name:
        num = base_name.replace("-O", "")
        for case_number, case_data in all_cases_by_number.items():
            if f"/O/{num}" in case_number:
                return case_data

    return None


def build_cas_laudo_row(
    doc_id: str,
    case_data: dict,
    pdf_name: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Build scraper_documents + cas_laudos row data from JSON metadata.

    Returns (parent_data, child_data) tuple ready for DB insertion.
    """
    case_number = case_data.get("case_number", "").strip()
    parsed = parse_case_number(case_number)

    # Parse date if available
    date_str = case_data.get("decision_date", "")
    fecha_laudo = None
    if date_str:
        for fmt in ("%Y-%m-%d", "%d %B %Y", "%B %d, %Y"):
            try:
                fecha_laudo = datetime.strptime(date_str, fmt).date().isoformat()
                break
            except ValueError:
                continue

    # Build parties
    partes = []
    for appellant in case_data.get("appellants", []):
        name = clean_party_name(appellant)
        if name:
            partes.append({"nombre": name, "tipo": "appellant", "pais": None})

    for respondent in case_data.get("respondents", []):
        name = clean_party_name(respondent)
        if name:
            partes.append({"nombre": name, "tipo": "respondent", "pais": None})

    # Build arbitrators (filter out "//" placeholders)
    arbitros = []
    for arb in case_data.get("arbitrators", []):
        arb_name = arb.strip()
        if arb_name and arb_name != "//":
            arbitros.append({"nombre": arb_name, "nacionalidad": "", "rol": None})

    # Map sport string to enum value (best effort)
    sport_raw = (case_data.get("sport") or "").strip().lower()
    sport_map = {
        "football": "football",
        "athletics": "athletics",
        "cycling": "cycling",
        "swimming": "swimming",
        "basketball": "basketball",
        "tennis": "tennis",
        "skiing": "skiing",
        "weightlifting": "other",
    }
    categoria_deporte = sport_map.get(sport_raw)

    # Determine procedure type
    proc_type = procedure_type_from_code(parsed.get("procedure"))

    # Build outcome/summary
    outcome = case_data.get("outcome", "")
    resumen = f"Outcome: {outcome}" if outcome else None

    # Full text (some JSON files include it)
    full_text = case_data.get("full_text")

    parent_data = {
        "id": doc_id,
        "source_type": "cas",
        "external_id": case_number,
        "title": case_data.get("title", case_number),
        "publication_date": fecha_laudo,
        "storage_path": f"{BUCKET_NAME}/{pdf_name}",
    }

    child_data = {
        "id": doc_id,
        "numero_caso": case_number,
        "fecha_laudo": fecha_laudo,
        "tipo_procedimiento": proc_type,
        "categoria_deporte": categoria_deporte,
        "materia": None,
        "idioma": case_data.get("language") or None,
        "estado": "published",
        "resumen": resumen,
        "texto_completo": full_text,
        "partes": partes,
        "arbitros": arbitros,
        "federaciones": [],
        "palabras_clave": case_data.get("keywords", []) or [],
        "pdf_storage_path": f"{BUCKET_NAME}/{pdf_name}",
        "chunk_count": 0,
        "embedding_status": "pending",
    }

    return parent_data, child_data


def build_minimal_row(
    doc_id: str,
    pdf_name: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Build minimal row for a PDF with no JSON metadata.

    Derives case number from filename where possible.
    """
    base = pdf_name.replace(".pdf", "")

    # Derive case number from filename
    if base.startswith("ADD"):
        inner = base[3:]
        # Try to map: ADD2059 → CAS 20XX/ADD/59
        # We don't know the year, use placeholder
        case_number = f"CAS/ADD/{inner}"
    elif "-O" in base:
        num = base.replace("-O", "")
        case_number = f"CAS/O/{num}"
    else:
        nums = extract_numbers_from_pdf_name(pdf_name)
        if len(nums) > 1:
            case_number = f"CAS/A/{nums[0]}-{nums[-1]}"
        else:
            case_number = f"CAS/A/{nums[0]}" if nums else f"CAS/{base}"

    parent_data = {
        "id": doc_id,
        "source_type": "cas",
        "external_id": case_number,
        "title": case_number,
        "publication_date": None,
        "storage_path": f"{BUCKET_NAME}/{pdf_name}",
    }

    child_data = {
        "id": doc_id,
        "numero_caso": case_number,
        "fecha_laudo": None,
        "tipo_procedimiento": None,
        "categoria_deporte": None,
        "materia": None,
        "idioma": None,
        "estado": "published",
        "resumen": None,
        "texto_completo": None,
        "partes": [],
        "arbitros": [],
        "federaciones": [],
        "palabras_clave": [],
        "pdf_storage_path": f"{BUCKET_NAME}/{pdf_name}",
        "chunk_count": 0,
        "embedding_status": "pending",
    }

    return parent_data, child_data


# ---------------------------------------------------------------------------
# Main ingestion logic
# ---------------------------------------------------------------------------

def ingest(
    client: Client,
    dry_run: bool = False,
    register_docs: bool = False,
) -> dict[str, int]:
    """
    Run the ingestion pipeline.

    Returns stats dict with counts of ingested, skipped, errors.
    """
    stats = {"ingested": 0, "skipped": 0, "errors": 0, "registered": 0}

    # 1. List all files in bucket
    print(f"[Ingest] Listing files in '{BUCKET_NAME}' bucket...")
    files = client.storage.from_(BUCKET_NAME).list()
    pdf_files = sorted([f["name"] for f in files if f["name"].endswith(".pdf")])
    json_files = [f["name"] for f in files if f["name"].endswith(".json")]
    print(f"[Ingest] Found {len(pdf_files)} PDFs, {len(json_files)} JSONs")

    # 2. Load all_cases.json as primary metadata index
    all_cases: list[dict] = []
    if "all_cases.json" in json_files:
        print("[Ingest] Loading all_cases.json index...")
        all_cases = download_json_from_bucket(client, "all_cases.json")
        print(f"[Ingest] Loaded {len(all_cases)} case entries from index")

    # Build lookup by case_number
    cases_by_number: dict[str, dict] = {}
    for case in all_cases:
        cn = case.get("case_number", "").strip()
        if cn:
            cases_by_number[cn] = case

    # 3. Load individual JSON files for cases that might have full_text
    # (all_cases.json may not include full_text)
    individual_jsons: dict[str, dict] = {}
    for jf in json_files:
        if jf in ("all_cases.json", "nlp_dataset_ready.json", "nlp_dataset_sample.json"):
            continue
        try:
            data = download_json_from_bucket(client, jf)
            cn = data.get("case_number", "").strip()
            if cn:
                individual_jsons[cn] = data
        except Exception as e:
            print(f"[Ingest] WARNING: Failed to parse {jf}: {e}")

    print(f"[Ingest] Loaded {len(individual_jsons)} individual JSON metadata files")

    # Merge full_text from individual JSONs into cases_by_number
    for cn, indiv_data in individual_jsons.items():
        if cn in cases_by_number:
            # If individual JSON has full_text but index doesn't, add it
            if indiv_data.get("full_text") and not cases_by_number[cn].get("full_text"):
                cases_by_number[cn]["full_text"] = indiv_data["full_text"]
        else:
            # Case not in index — add it
            cases_by_number[cn] = indiv_data

    # 4. Check existing cas_laudos to skip duplicates
    existing = (
        client.table("cas_laudos")
        .select("numero_caso")
        .execute()
    )
    existing_cases = {row["numero_caso"] for row in existing.data}
    print(f"[Ingest] {len(existing_cases)} cases already in database")

    # 5. Process each PDF
    total = len(pdf_files)
    for i, pdf_name in enumerate(pdf_files, 1):
        prefix = f"[{i}/{total}]"

        # Try to match PDF to metadata
        case_data = match_pdf_to_case_number(pdf_name, cases_by_number)

        if case_data:
            case_number = case_data.get("case_number", "").strip()
        else:
            # No metadata — derive case number from filename
            nums = extract_numbers_from_pdf_name(pdf_name)
            base = pdf_name.replace(".pdf", "")
            if base.startswith("ADD"):
                case_number = f"CAS/ADD/{base[3:]}"
            elif "-O" in base:
                case_number = f"CAS/O/{base.replace('-O', '')}"
            elif len(nums) > 1:
                case_number = f"CAS/A/{nums[0]}-{nums[-1]}"
            else:
                case_number = f"CAS/A/{nums[0]}" if nums else f"CAS/{base}"

        # Skip if already ingested
        if case_number in existing_cases:
            print(f"{prefix} SKIP {pdf_name} → {case_number} (already exists)")
            stats["skipped"] += 1
            continue

        # Generate UUID
        doc_id = str(uuid.uuid4())

        # Build row data
        if case_data:
            parent_data, child_data = build_cas_laudo_row(doc_id, case_data, pdf_name)
        else:
            parent_data, child_data = build_minimal_row(doc_id, pdf_name)
            print(f"{prefix} WARN  {pdf_name} → {case_number} (no JSON metadata)")

        if dry_run:
            parties_count = len(child_data.get("partes", []))
            has_text = "yes" if child_data.get("texto_completo") else "no"
            print(
                f"{prefix} DRY   {pdf_name} → {case_number} "
                f"(parties={parties_count}, full_text={has_text})"
            )
            stats["ingested"] += 1
            continue

        # Insert into database
        try:
            # Insert parent (scraper_documents)
            client.table("scraper_documents").insert(parent_data).execute()

            # Insert child (cas_laudos)
            client.table("cas_laudos").insert(child_data).execute()

            parties_count = len(child_data.get("partes", []))
            has_text = "yes" if child_data.get("texto_completo") else "no"
            print(
                f"{prefix} OK    {pdf_name} → {case_number} "
                f"(parties={parties_count}, full_text={has_text})"
            )
            stats["ingested"] += 1
            existing_cases.add(case_number)

            # Optionally register in document_registry
            if register_docs:
                try:
                    registry_data = {
                        "source_category": "scraper",
                        "source_type": "cas",
                        "external_id": case_number,
                        "title": parent_data.get("title", case_number),
                        "file_name": pdf_name,
                        "mime_type": "application/pdf",
                        "storage_bucket": BUCKET_NAME,
                        "storage_path": pdf_name,
                        "processing_status": "pending",
                    }
                    client.table("document_registry").insert(registry_data).execute()
                    stats["registered"] += 1
                except Exception as reg_err:
                    print(f"{prefix} WARN  document_registry insert failed: {reg_err}")

        except Exception as e:
            err_msg = str(e)
            # Handle unique constraint violation (already exists via different case_number format)
            if "unique_source_document" in err_msg or "cas_laudos_numero_caso_key" in err_msg:
                print(f"{prefix} SKIP  {pdf_name} → {case_number} (duplicate)")
                stats["skipped"] += 1
            else:
                print(f"{prefix} ERROR {pdf_name} → {case_number}: {err_msg}")
                stats["errors"] += 1

    return stats


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    dry_run = "--dry-run" in sys.argv
    register_docs = "--register-docs" in sys.argv

    if dry_run:
        print("=" * 60)
        print("DRY RUN MODE — no database changes will be made")
        print("=" * 60)

    print()
    print("CAS Bucket Ingestion")
    print(f"  Bucket: {BUCKET_NAME}")
    print(f"  Supabase URL: {SUPABASE_URL}")
    print(f"  Register in document_registry: {register_docs}")
    print()

    client = get_supabase_client()

    stats = ingest(client, dry_run=dry_run, register_docs=register_docs)

    print()
    print("=" * 60)
    print("SUMMARY")
    print(f"  Ingested: {stats['ingested']}")
    print(f"  Skipped:  {stats['skipped']}")
    print(f"  Errors:   {stats['errors']}")
    if register_docs:
        print(f"  Registered in document_registry: {stats['registered']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
