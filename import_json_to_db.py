#!/usr/bin/env python3
"""Import existing JSON files into Supabase database."""

import json
import os
import uuid
from datetime import datetime
from pathlib import Path
import asyncio
import asyncpg

# Database connection from environment
DB_HOST = os.getenv("SUPABASE_DB_HOST", "legaltracking-dev-db-1")
DB_PORT = int(os.getenv("SUPABASE_DB_PORT", "5432"))
DB_NAME = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD", "")
DB_USER = "postgres"

# Paths
SCRAPED_DATA_DIR = Path("/root/legal-scraper/scraped_data")
SCJN_DATA_DIR = Path("/root/legal-scraper/scjn_data/documents")


async def import_dof_documents(conn):
    """Import DOF documents from scraped_data/."""
    print("Importing DOF documents...")
    count = 0
    errors = 0

    for json_file in SCRAPED_DATA_DIR.glob("*.json"):
        try:
            with open(json_file) as f:
                data = json.load(f)

            doc_id = str(uuid.uuid4())
            external_id = f"dof-{json_file.stem[:50]}"

            # Insert into scraper_documents
            await conn.execute("""
                INSERT INTO scraper_documents (id, source_type, external_id, title, publication_date)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (source_type, external_id) DO NOTHING
            """, doc_id, "dof", external_id,
                data.get("title", json_file.stem)[:500],
                datetime.strptime(data.get("publication_date", "2021-01-01"), "%Y-%m-%d").date() if data.get("publication_date") else None)

            # Insert into dof_publicaciones
            await conn.execute("""
                INSERT INTO dof_publicaciones (id, dof_date, section, jurisdiction, articles, full_text)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (id) DO NOTHING
            """, doc_id,
                datetime.strptime(data.get("publication_date", "2021-01-01"), "%Y-%m-%d").date() if data.get("publication_date") else None,
                "primera",
                data.get("jurisdiction", "Federal"),
                json.dumps(data.get("articles", [])),
                data.get("full_text", ""))

            count += 1
        except Exception as e:
            print(f"Error importing {json_file.name}: {e}")
            errors += 1

    print(f"DOF: Imported {count} documents, {errors} errors")
    return count


async def import_scjn_documents(conn):
    """Import SCJN documents from scjn_data/documents/."""
    print("Importing SCJN documents...")
    count = 0
    errors = 0

    for json_file in SCJN_DATA_DIR.glob("*.json"):
        try:
            with open(json_file) as f:
                data = json.load(f)

            if data.get("test"):  # Skip test.json
                continue

            doc_id = str(uuid.uuid4())
            external_id = data.get("q_param", json_file.stem)[:100]

            # Insert into scraper_documents
            await conn.execute("""
                INSERT INTO scraper_documents (id, source_type, external_id, title, publication_date)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (source_type, external_id) DO NOTHING
            """, doc_id, "scjn", external_id,
                data.get("title", "")[:500],
                datetime.strptime(data.get("publication_date", "2026-01-01"), "%Y-%m-%d").date() if data.get("publication_date") else None)

            # Insert into scjn_documents
            await conn.execute("""
                INSERT INTO scjn_documents (id, q_param, short_title, category, scope, status, source_url)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (id) DO NOTHING
            """, doc_id,
                data.get("q_param", ""),
                data.get("title", "")[:100],
                data.get("category", "LEY"),
                data.get("scope", "FEDERAL"),
                data.get("status", "VIGENTE"),
                data.get("source_url", ""))

            count += 1
        except Exception as e:
            print(f"Error importing {json_file.name}: {e}")
            errors += 1

    print(f"SCJN: Imported {count} documents, {errors} errors")
    return count


async def main():
    print(f"Connecting to database at {DB_HOST}:{DB_PORT}/{DB_NAME}...")

    conn = await asyncpg.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

    try:
        dof_count = await import_dof_documents(conn)
        scjn_count = await import_scjn_documents(conn)

        print(f"\nTotal: {dof_count + scjn_count} documents imported")

        # Verify counts
        result = await conn.fetch("""
            SELECT source_type, COUNT(*) as count
            FROM scraper_documents
            GROUP BY source_type
        """)
        print("\nDatabase counts:")
        for row in result:
            print(f"  {row['source_type']}: {row['count']}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
