import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import src.gui.infrastructure.mounted_disk_backfill as mounted_disk_backfill

from src.gui.infrastructure.mounted_disk_backfill import (
    _count_documents_missing_storage,
    _update_document_row,
    backfill_source_from_mounted_disk,
    backfill_source_from_mounted_disk_until_exhausted,
    bootstrap_source_documents_from_mounted_disk,
    resolve_local_source_file,
)


class _DocsQuery:
    def __init__(self, client, table_name):
        self._client = client
        self._table_name = table_name
        self._filters = {}
        self._is_filters = {}
        self._not_is_filters = {}
        self._range = None
        self._update_values = None
        self._upsert_values = None
        self._invert_next = False
        self._count = None
        self._head = False
        self._limit = None
        self.range_calls = 0

    def select(self, _columns, count=None, head=False):
        self._count = count
        self._head = head
        return self

    def eq(self, key, value):
        self._filters[key] = value
        return self

    @property
    def not_(self):
        self._invert_next = True
        return self

    def is_(self, key, value):
        if self._invert_next:
            self._not_is_filters[key] = value
            self._invert_next = False
        else:
            self._is_filters[key] = value
        return self

    def order(self, *_args, **_kwargs):
        return self

    def range(self, start, end):
        self._range = (start, end)
        self.range_calls += 1
        return self

    def limit(self, value):
        self._limit = value
        return self

    def update(self, values):
        self._update_values = values
        return self

    def upsert(self, values, on_conflict=None):
        self._upsert_values = values
        self._on_conflict = on_conflict
        return self

    def execute(self):
        data = self._client.rows_by_table[self._table_name]
        if self._upsert_values is not None:
            payloads = self._upsert_values if isinstance(self._upsert_values, list) else [self._upsert_values]
            results = []
            for payload in payloads:
                payload = dict(payload)
                if self._table_name == "scraper_documents":
                    key_fields = ("source_type", "external_id")
                elif self._table_name == "scraper_document_aliases":
                    key_fields = ("source_type", "alias_value")
                else:
                    key_fields = tuple()
                existing = None
                for row in data:
                    if key_fields and all(row.get(key) == payload.get(key) for key in key_fields):
                        existing = row
                        break
                if existing is None:
                    if self._table_name == "scraper_documents" and not payload.get("id"):
                        payload["id"] = f"{payload.get('source_type', 'doc')}-{payload.get('external_id')}"
                    data.append(payload)
                    existing = payload
                else:
                    existing.update(payload)
                results.append(dict(existing))
            return SimpleNamespace(data=results)

        data = self._rows
        for key, value in self._filters.items():
            data = [row for row in data if row.get(key) == value]
        for key, value in self._is_filters.items():
            if value == "null":
                data = [row for row in data if row.get(key) is None]
        for key, value in self._not_is_filters.items():
            if value == "null":
                data = [row for row in data if row.get(key) is not None]

        if self._update_values is not None:
            for row in data:
                row.update(self._update_values)
            return SimpleNamespace(data=data)

        count = len(data) if self._count == "exact" else None
        if self._head:
            return SimpleNamespace(data=[], count=count)

        if self._limit is not None:
            data = data[: self._limit]
        if self._range is not None:
            start, end = self._range
            data = data[start : end + 1]
        return SimpleNamespace(data=data, count=count)


class _FakeSupabaseClient:
    def __init__(self, rows, alias_rows=None):
        self.rows_by_table = {
            "scraper_documents": rows,
            "scraper_document_aliases": alias_rows if alias_rows is not None else [],
        }
        self.queries = []

    def table(self, name):
        assert name in self.rows_by_table
        query = _DocsQuery(self, name)
        query._rows = self.rows_by_table[name]
        self.queries.append(query)
        return query


class _FakeStorageAdapter:
    def __init__(self):
        self.upload_calls = []
        self.existing_paths = set()
        self.raise_on_doc_ids = {}

    async def upload(
        self,
        content,
        source_type,
        doc_id,
        content_type="application/pdf",
        file_extension=None,
        tenant_id=None,
    ):
        exc = self.raise_on_doc_ids.get(doc_id)
        if exc is not None:
            raise exc
        self.upload_calls.append(
            {
                "content": content,
                "source_type": source_type,
                "doc_id": doc_id,
                "content_type": content_type,
                "file_extension": file_extension,
            }
        )
        suffix = file_extension or (".txt" if content_type == "text/plain" else ".pdf")
        return f"{source_type}/ab/{doc_id}{suffix}"

    def build_path(
        self,
        source_type,
        doc_id,
        content_type="application/pdf",
        file_extension=None,
        tenant_id=None,
    ):
        suffix = file_extension or (".txt" if content_type == "text/plain" else ".pdf")
        return f"{source_type}/ab/{doc_id}{suffix}"

    async def exists(self, path):
        return path in self.existing_paths


class _FakeTemporalClient:
    def __init__(self):
        self.calls = []

    async def trigger_embedding_workflow(
        self,
        job_id,
        source_type,
        document_count,
        document_ids=None,
        batch_size=10,
        max_documents=0,
    ):
        self.calls.append(
            {
                "job_id": job_id,
                "source_type": source_type,
                "document_count": document_count,
                "document_ids": list(document_ids or []),
                "batch_size": batch_size,
                "max_documents": max_documents,
            }
        )
        return f"run-{job_id}"


def test_update_document_row_stamps_updated_at():
    rows = [
        {
            "id": "doc-1",
            "external_id": "7867",
            "title": "Mounted book",
            "source_type": "bjv",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
        }
    ]
    client = _FakeSupabaseClient(rows)

    _update_document_row(client, "doc-1", {"embedding_status": "processing"})

    assert rows[0]["embedding_status"] == "processing"
    assert "updated_at" in rows[0]


def test_resolve_local_source_file_supports_source_specific_mappings(tmp_path):
    (tmp_path / "books" / "7897" / "chapters").mkdir(parents=True)
    (tmp_path / "books" / "7897" / "chapters" / "6_7897.pdf").write_bytes(b"bjv-chapter")

    (tmp_path / "cas_pdfs").mkdir()
    (tmp_path / "cas_pdfs" / "378-O.pdf").write_bytes(b"cas-o")
    (tmp_path / "cas_pdfs" / "378.pdf").write_bytes(b"cas-a")
    (tmp_path / "cas" / "converted" / "A2-99").mkdir(parents=True)
    (tmp_path / "cas" / "converted" / "A2-99" / "canonical.json").write_text(
        '{"sections":[{"text":"Appeal reasoning","type":"paragraph","heading_path":["Reasons"],"blocks":[{"page":2}]}]}',
        encoding="utf-8",
    )
    (tmp_path / "cas" / "converted" / "1").mkdir(parents=True)
    (tmp_path / "cas" / "converted" / "1" / "output.txt").write_text(
        "cas converted olympic",
        encoding="utf-8",
    )

    (tmp_path / "federal" / "converted" / "wo45").mkdir(parents=True)
    (tmp_path / "federal" / "converted" / "wo45" / "output.txt").write_text("orden", encoding="utf-8")

    (tmp_path / "estatal" / "17" / "doc").mkdir(parents=True)
    (tmp_path / "estatal" / "17" / "doc" / "comp_63976.doc").write_bytes(b"orden-doc")

    (tmp_path / "tesis" / "pdf").mkdir(parents=True)
    (tmp_path / "tesis" / "pdf" / "288180.pdf").write_bytes(b"scjn")
    (tmp_path / "tesis" / "converted" / "288180").mkdir(parents=True)
    (tmp_path / "tesis" / "converted" / "288180" / "output.txt").write_text(
        "scjn convertido",
        encoding="utf-8",
    )

    (tmp_path / "dof" / "converted" / "5784621").mkdir(parents=True)
    (tmp_path / "dof" / "converted" / "5784621" / "output.txt").write_text(
        "dof convertido",
        encoding="utf-8",
    )
    (tmp_path / "dof" / "5784621.doc").write_bytes(b"legacy doc")

    cas_o = resolve_local_source_file(
        "cas",
        "000001b8-0000-0000-0000-000000000000",
        title="CAS 1998/O/378 Celtic Football Club v. UEFA — Football, Transfer",
        data_root=tmp_path,
    )
    cas_a = resolve_local_source_file(
        "cas",
        "0000092c-0000-0000-0000-000000000000",
        title="CAS A2/99 AOC v. E. et al. — Boxing, Doping",
        data_root=tmp_path,
    )
    cas_og = resolve_local_source_file(
        "cas",
        "00000956-0000-0000-0000-000000000000",
        title="CAS OG 00/001 USOC v. IOC et al. — Canoe, Nationality",
        data_root=tmp_path,
    )
    bjv_chapter = resolve_local_source_file("bjv", "book-7897-ch6", data_root=tmp_path)
    dof = resolve_local_source_file("dof", "5784621", data_root=tmp_path)
    orden_direct = resolve_local_source_file("orden", "comp_63976", data_root=tmp_path)
    orden_converted = resolve_local_source_file("orden", "wo45", data_root=tmp_path)
    scjn = resolve_local_source_file("scjn", "tesis-288180", data_root=tmp_path)

    assert cas_o is not None and cas_o.path.name == "378-O.pdf"
    assert cas_a is not None and cas_a.path.name == "canonical.json"
    assert cas_og is not None and cas_og.path.name == "output.txt"
    assert bjv_chapter is not None and bjv_chapter.path.name == "6_7897.pdf"
    assert dof is not None and dof.path.name == "output.txt"
    assert orden_direct is not None and orden_direct.path.name == "comp_63976.doc"
    assert orden_converted is not None and orden_converted.path.name == "output.txt"
    assert scjn is not None and scjn.path.name == "output.txt"


def test_resolve_local_source_file_supports_multiple_scjn_families(tmp_path):
    (tmp_path / "ejecutorias" / "converted" / "33943").mkdir(parents=True)
    (tmp_path / "ejecutorias" / "converted" / "33943" / "output.txt").write_text(
        "scjn ejecutoria",
        encoding="utf-8",
    )
    (tmp_path / "votos" / "pdf").mkdir(parents=True)
    (tmp_path / "votos" / "pdf" / "300161.pdf").write_bytes(b"vote-pdf")

    ejecutoria = resolve_local_source_file("scjn", "ejecutorias-33943", data_root=tmp_path)
    voto = resolve_local_source_file("scjn", "votos-300161", data_root=tmp_path)

    assert ejecutoria is not None and ejecutoria.path.name == "output.txt"
    assert voto is not None and voto.path.name == "300161.pdf"


def test_resolve_local_source_file_prefers_scjn_converted_then_api_json_then_pdf(tmp_path):
    (tmp_path / "ejecutorias" / "converted" / "33943").mkdir(parents=True)
    (tmp_path / "ejecutorias" / "converted" / "33943" / "output.txt").write_text(
        "converted ejecutoria",
        encoding="utf-8",
    )
    (tmp_path / "ejecutorias" / "api_json").mkdir(parents=True)
    (tmp_path / "ejecutorias" / "api_json" / "33943.json").write_text(
        '{"rubro":"Ejecutoria"}',
        encoding="utf-8",
    )
    (tmp_path / "ejecutorias" / "pdf").mkdir(parents=True)
    (tmp_path / "ejecutorias" / "pdf" / "33943.pdf").write_bytes(b"ejecutoria-pdf")

    (tmp_path / "votos" / "api_json").mkdir(parents=True)
    (tmp_path / "votos" / "api_json" / "47404.json").write_text(
        '{"rubro":"Voto"}',
        encoding="utf-8",
    )
    (tmp_path / "votos" / "pdf").mkdir(parents=True)
    (tmp_path / "votos" / "pdf" / "47404.pdf").write_bytes(b"vote-pdf")

    ejecutoria = resolve_local_source_file("scjn", "ejecutorias-33943", data_root=tmp_path)
    voto = resolve_local_source_file("scjn", "votos-47404", data_root=tmp_path)

    assert ejecutoria is not None and ejecutoria.path.name == "output.txt"
    assert voto is not None and voto.path.name == "47404.json"


def test_resolve_local_source_file_does_not_prefix_match_unrelated_cas_ids(tmp_path):
    (tmp_path / "cas_pdfs").mkdir()
    (tmp_path / "cas_pdfs" / "400.pdf").write_bytes(b"wrong-match")

    cas = resolve_local_source_file(
        "cas",
        "000009c1-0000-0000-0000-000000000000",
        title="CAS OG 98/004 & 005 Czech Olympic Committee v. IIHF et al.",
        data_root=tmp_path,
    )

    assert cas is None


def test_bootstrap_source_documents_registers_mounted_rows_and_aliases(tmp_path):
    (tmp_path / "books" / "7867").mkdir(parents=True)
    (tmp_path / "books" / "7867" / "book.pdf").write_bytes(b"%PDF-1.4 mounted book")
    (tmp_path / "books" / "7867" / "chapters").mkdir()
    (tmp_path / "books" / "7867" / "chapters" / "6_7867.pdf").write_bytes(b"chapter")

    docs = []
    alias_rows = []
    supabase_client = _FakeSupabaseClient(docs, alias_rows=alias_rows)

    result = bootstrap_source_documents_from_mounted_disk(
        "bjv",
        data_root=tmp_path,
        supabase_client=supabase_client,
    )

    external_ids = {row["external_id"] for row in docs}
    alias_values = {row["alias_value"] for row in alias_rows}

    assert result["registered_rows"] == 2
    assert "7867" in external_ids
    assert "book-7867-ch6" in external_ids
    assert "book:7867" in alias_values
    assert "6_7867" in alias_values


def test_bootstrap_limit_counts_new_registrations_not_existing_rows(tmp_path):
    for thesis_id in ("2031990", "2031991", "2031992"):
        (tmp_path / "tesis" / "converted" / thesis_id).mkdir(parents=True, exist_ok=True)
        (tmp_path / "tesis" / "converted" / thesis_id / "output.txt").write_text(
            f"tesis {thesis_id}",
            encoding="utf-8",
        )

    docs = [
        {
            "id": "doc-1",
            "external_id": "tesis-2031990",
            "title": "tesis-2031990",
            "source_type": "scjn",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
        },
        {
            "id": "doc-2",
            "external_id": "tesis-2031991",
            "title": "tesis-2031991",
            "source_type": "scjn",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
        },
    ]

    supabase_client = _FakeSupabaseClient(docs, alias_rows=[])

    result = bootstrap_source_documents_from_mounted_disk(
        "scjn",
        data_root=tmp_path,
        supabase_client=supabase_client,
        limit=1,
    )

    external_ids = {row["external_id"] for row in docs}
    upsert_queries = [
        query
        for query in supabase_client.queries
        if query._table_name == "scraper_documents" and query._upsert_values is not None
    ]

    assert result["registered_rows"] == 1
    assert "tesis-2031992" in external_ids
    assert len(upsert_queries) == 1


def test_bootstrap_source_documents_registers_scjn_api_json_records(tmp_path):
    (tmp_path / "ejecutorias" / "api_json").mkdir(parents=True)
    (tmp_path / "ejecutorias" / "api_json" / "33873.json").write_text(
        '{"rubro":"Ejecutoria 33873"}',
        encoding="utf-8",
    )
    (tmp_path / "acuerdos" / "api_json").mkdir(parents=True)
    (tmp_path / "acuerdos" / "api_json" / "6126.json").write_text(
        '{"rubro":"Acuerdo 6126"}',
        encoding="utf-8",
    )

    docs = []
    supabase_client = _FakeSupabaseClient(docs, alias_rows=[])

    result = bootstrap_source_documents_from_mounted_disk(
        "scjn",
        data_root=tmp_path,
        supabase_client=supabase_client,
    )

    external_ids = {row["external_id"] for row in docs}

    assert result["registered_rows"] == 2
    assert external_ids == {"ejecutorias-33873", "acuerdos-6126"}


def test_bootstrap_scjn_prioritizes_api_only_families_before_tesis(tmp_path):
    (tmp_path / "tesis" / "converted" / "2031990").mkdir(parents=True)
    (tmp_path / "tesis" / "converted" / "2031990" / "output.txt").write_text(
        "tesis 2031990",
        encoding="utf-8",
    )
    (tmp_path / "ejecutorias" / "api_json").mkdir(parents=True)
    (tmp_path / "ejecutorias" / "api_json" / "33873.json").write_text(
        '{"rubro":"Ejecutoria 33873"}',
        encoding="utf-8",
    )

    docs = []
    supabase_client = _FakeSupabaseClient(docs, alias_rows=[])

    result = bootstrap_source_documents_from_mounted_disk(
        "scjn",
        data_root=tmp_path,
        supabase_client=supabase_client,
        limit=1,
    )

    assert result["registered_rows"] == 1
    assert docs[0]["external_id"] == "ejecutorias-33873"


@pytest.mark.asyncio
async def test_backfill_hydrates_before_bootstrap(monkeypatch, tmp_path):
    (tmp_path / "books" / "7867").mkdir(parents=True)
    (tmp_path / "books" / "7867" / "book.pdf").write_bytes(b"%PDF-1.4 mounted book")

    docs = [
        {
            "id": "doc-1",
            "external_id": "7867",
            "title": "Mounted book",
            "source_type": "bjv",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-11T00:00:00+00:00",
        }
    ]

    call_order = []
    original_persist = mounted_disk_backfill._persist_local_match

    async def tracked_persist(*args, **kwargs):
        call_order.append("hydrate")
        return await original_persist(*args, **kwargs)

    def tracked_bootstrap(*args, **kwargs):
        call_order.append("bootstrap")
        return {
            "registered_rows": 0,
            "existing_registered_rows": 0,
            "alias_rows_upserted": 0,
            "errors": [],
        }

    monkeypatch.setattr(mounted_disk_backfill, "_persist_local_match", tracked_persist)
    monkeypatch.setattr(
        mounted_disk_backfill,
        "bootstrap_source_documents_from_mounted_disk",
        tracked_bootstrap,
    )

    supabase_client = _FakeSupabaseClient(docs)
    storage_adapter = _FakeStorageAdapter()
    temporal_client = _FakeTemporalClient()

    result = await backfill_source_from_mounted_disk(
        "bjv",
        limit=10,
        data_root=tmp_path,
        supabase_client=supabase_client,
        storage_adapter=storage_adapter,
        temporal_client=temporal_client,
        trigger_embedding=False,
    )

    assert result.uploaded == 0
    assert result.reused_storage_object == 1
    assert call_order == ["hydrate", "bootstrap"]


@pytest.mark.asyncio
async def test_backfill_source_uploads_missing_storage_and_requeues_existing_storage(tmp_path):
    (tmp_path / "books" / "7867").mkdir(parents=True)
    (tmp_path / "books" / "7867" / "book.pdf").write_bytes(b"%PDF-1.4 mounted book")

    docs = [
        {
            "id": "doc-1",
            "external_id": "7867",
            "title": "Mounted book",
            "source_type": "bjv",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-11T00:00:00+00:00",
        },
        {
            "id": "doc-2",
            "external_id": "7439",
            "title": "Already in storage",
            "source_type": "bjv",
            "storage_path": "bjv/existing/7439.html",
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-10T00:00:00+00:00",
        },
        {
            "id": "doc-3",
            "external_id": "9999",
            "title": "Missing local source",
            "source_type": "bjv",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-09T00:00:00+00:00",
        },
        {
            "id": "doc-4",
            "external_id": "7000",
            "title": "Already complete",
            "source_type": "bjv",
            "storage_path": "bjv/existing/7000.pdf",
            "embedding_status": "completed",
            "chunk_count": 4,
            "created_at": "2026-04-08T00:00:00+00:00",
        },
    ]

    supabase_client = _FakeSupabaseClient(docs)
    storage_adapter = _FakeStorageAdapter()
    storage_adapter.existing_paths.add("bjv/existing/7439.html")
    temporal_client = _FakeTemporalClient()

    result = await backfill_source_from_mounted_disk(
        "bjv",
        limit=10,
        data_root=tmp_path,
        bootstrap_missing_documents=False,
        supabase_client=supabase_client,
        storage_adapter=storage_adapter,
        temporal_client=temporal_client,
    )

    assert result.uploaded == 0
    assert result.reused_storage_object == 1
    assert result.queued_existing_storage == 1
    assert result.missing_local_file == 1
    assert result.already_completed == 1
    assert result.queued_for_embedding == 2
    assert docs[0]["storage_path"] == "mounted://books/7867/book.pdf"
    assert docs[0]["embedding_status"] == "pending"
    assert docs[2]["embedding_status"] == "awaiting_source_file"
    assert temporal_client.calls[0]["document_ids"] == ["doc-2", "doc-1"]


@pytest.mark.asyncio
async def test_backfill_source_replaces_stale_storage_path_when_local_file_exists(tmp_path):
    (tmp_path / "books" / "7867").mkdir(parents=True)
    (tmp_path / "books" / "7867" / "book.pdf").write_bytes(b"%PDF-1.4 mounted book")

    docs = [
        {
            "id": "doc-1",
            "external_id": "7867",
            "title": "Mounted book",
            "source_type": "bjv",
            "storage_path": "legacy/7867.pdf",
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-11T00:00:00+00:00",
        }
    ]

    supabase_client = _FakeSupabaseClient(docs)
    storage_adapter = _FakeStorageAdapter()
    temporal_client = _FakeTemporalClient()

    result = await backfill_source_from_mounted_disk(
        "bjv",
        limit=10,
        data_root=tmp_path,
        bootstrap_missing_documents=False,
        supabase_client=supabase_client,
        storage_adapter=storage_adapter,
        temporal_client=temporal_client,
    )

    assert result.uploaded == 0
    assert result.reused_storage_object == 1
    assert result.queued_existing_storage == 0
    assert result.queued_for_embedding == 1
    assert docs[0]["storage_path"] == "mounted://books/7867/book.pdf"


@pytest.mark.asyncio
async def test_backfill_filters_storage_stage_at_query_time(tmp_path):
    docs = [
        {
            "id": "doc-missing-1",
            "external_id": "tesis-1001",
            "title": "Missing local source 1",
            "source_type": "scjn",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-11T00:00:00+00:00",
        },
        {
            "id": "doc-missing-2",
            "external_id": "tesis-1002",
            "title": "Missing local source 2",
            "source_type": "scjn",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-10T00:00:00+00:00",
        },
        {
            "id": "doc-existing",
            "external_id": "tesis-288180",
            "title": "Existing storage",
            "source_type": "scjn",
            "storage_path": "scjn/existing/288180.pdf",
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-09T00:00:00+00:00",
        },
    ]

    supabase_client = _FakeSupabaseClient(docs)
    storage_adapter = _FakeStorageAdapter()
    storage_adapter.existing_paths.add("scjn/existing/288180.pdf")
    temporal_client = _FakeTemporalClient()

    result = await backfill_source_from_mounted_disk(
        "scjn",
        limit=1,
        data_root=tmp_path,
        supabase_client=supabase_client,
        storage_adapter=storage_adapter,
        temporal_client=temporal_client,
    )

    assert result.scanned == 1
    assert result.queued_existing_storage == 1
    assert result.queued_for_embedding == 1
    assert temporal_client.calls[0]["document_ids"] == ["doc-existing"]


@pytest.mark.asyncio
async def test_backfill_source_retriggers_pending_hydrated_docs_when_no_new_hydration(tmp_path):
    docs = [
        {
            "id": "doc-1",
            "external_id": "tesis-2031990",
            "title": "Tesis 2031990",
            "source_type": "scjn",
            "storage_path": "scjn/ab/tesis-2031990.txt",
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-11T00:00:00+00:00",
        }
    ]

    supabase_client = _FakeSupabaseClient(docs)
    storage_adapter = _FakeStorageAdapter()
    storage_adapter.existing_paths.add("scjn/ab/tesis-2031990.txt")
    temporal_client = _FakeTemporalClient()

    result = await backfill_source_from_mounted_disk(
        "scjn",
        limit=10,
        data_root=tmp_path,
        supabase_client=supabase_client,
        storage_adapter=storage_adapter,
        temporal_client=temporal_client,
    )

    assert result.queued_for_embedding == 1
    assert temporal_client.calls[0]["document_ids"] == ["doc-1"]
    assert storage_adapter.upload_calls == []


@pytest.mark.asyncio
async def test_backfill_skips_blocked_missing_source_rows_when_counting_limit(tmp_path):
    (tmp_path / "tesis" / "converted" / "288180").mkdir(parents=True)
    (tmp_path / "tesis" / "converted" / "288180" / "output.txt").write_text(
        "scjn convertido",
        encoding="utf-8",
    )

    docs = [
        {
            "id": "doc-missing",
            "external_id": "legacy-hash-without-mounted-match",
            "title": "Unresolvable first row",
            "source_type": "scjn",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-11T00:00:00+00:00",
        },
        {
            "id": "doc-hydratable",
            "external_id": "tesis-288180",
            "title": "Resolvable second row",
            "source_type": "scjn",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-10T00:00:00+00:00",
        },
    ]

    supabase_client = _FakeSupabaseClient(docs)
    storage_adapter = _FakeStorageAdapter()
    temporal_client = _FakeTemporalClient()

    result = await backfill_source_from_mounted_disk(
        "scjn",
        limit=1,
        data_root=tmp_path,
        supabase_client=supabase_client,
        storage_adapter=storage_adapter,
        temporal_client=temporal_client,
    )

    assert result.scanned == 2
    assert result.eligible == 1
    assert result.missing_local_file == 1
    assert result.queued_for_embedding == 1
    assert docs[0]["embedding_status"] == "awaiting_source_file"
    assert docs[1]["storage_path"] == "scjn/ab/tesis-288180.txt"
    assert temporal_client.calls[0]["document_ids"] == ["doc-hydratable"]


@pytest.mark.asyncio
async def test_backfill_source_retries_scjn_api_json_rows_from_awaiting_source(tmp_path):
    (tmp_path / "ejecutorias" / "api_json").mkdir(parents=True)
    (tmp_path / "ejecutorias" / "api_json" / "33873.json").write_text(
        '{"rubro":"Ejecutoria 33873","texto":"Texto ejecutoria"}',
        encoding="utf-8",
    )

    docs = [
        {
            "id": "doc-1",
            "external_id": "ejecutorias-33873",
            "title": "Ejecutoria 33873",
            "source_type": "scjn",
            "storage_path": None,
            "embedding_status": "awaiting_source_file",
            "chunk_count": 0,
            "updated_at": "2026-04-11T00:00:00+00:00",
            "created_at": "2026-04-10T00:00:00+00:00",
        }
    ]

    supabase_client = _FakeSupabaseClient(docs)
    storage_adapter = _FakeStorageAdapter()
    temporal_client = _FakeTemporalClient()

    result = await backfill_source_from_mounted_disk(
        "scjn",
        limit=10,
        data_root=tmp_path,
        bootstrap_missing_documents=False,
        supabase_client=supabase_client,
        storage_adapter=storage_adapter,
        temporal_client=temporal_client,
    )

    assert result.queued_for_embedding == 1
    assert docs[0]["storage_path"] == "mounted://ejecutorias/api_json/33873.json"
    assert docs[0]["embedding_status"] == "pending"
    assert temporal_client.calls[0]["document_ids"] == ["doc-1"]


@pytest.mark.asyncio
async def test_backfill_source_refreshes_existing_storage_when_local_format_is_better(tmp_path):
    (tmp_path / "dof" / "converted" / "5784621").mkdir(parents=True)
    (tmp_path / "dof" / "converted" / "5784621" / "output.txt").write_text(
        "dof convertido",
        encoding="utf-8",
    )
    (tmp_path / "dof" / "5784621.doc").write_bytes(b"legacy doc")

    docs = [
        {
            "id": "doc-1",
            "external_id": "5784621",
            "title": "Legacy DOF document",
            "source_type": "dof",
            "storage_path": "dof/51/5784621.doc",
            "embedding_status": "failed",
            "chunk_count": 0,
            "created_at": "2026-04-11T00:00:00+00:00",
        }
    ]

    supabase_client = _FakeSupabaseClient(docs)
    storage_adapter = _FakeStorageAdapter()
    storage_adapter.existing_paths.add("dof/51/5784621.doc")
    temporal_client = _FakeTemporalClient()

    result = await backfill_source_from_mounted_disk(
        "dof",
        limit=10,
        data_root=tmp_path,
        supabase_client=supabase_client,
        storage_adapter=storage_adapter,
        temporal_client=temporal_client,
    )

    assert result.uploaded == 1
    assert result.queued_existing_storage == 0
    assert docs[0]["storage_path"] == "dof/ab/5784621.txt"


@pytest.mark.asyncio
async def test_backfill_source_falls_back_to_mounted_path_when_upload_is_too_large(tmp_path):
    (tmp_path / "dof").mkdir(parents=True)
    local_doc = tmp_path / "dof" / "5728067.doc"
    local_doc.write_bytes(b"large legacy doc")

    docs = [
        {
            "id": "doc-1",
            "external_id": "5728067",
            "title": "Large DOF document",
            "source_type": "dof",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-11T00:00:00+00:00",
        }
    ]

    supabase_client = _FakeSupabaseClient(docs)
    storage_adapter = _FakeStorageAdapter()
    storage_adapter.raise_on_doc_ids["5728067"] = Exception(
        "{'statusCode': 413, 'error': Payload too large, 'message': The object exceeded the maximum allowed size}"
    )
    temporal_client = _FakeTemporalClient()

    result = await backfill_source_from_mounted_disk(
        "dof",
        limit=10,
        data_root=tmp_path,
        supabase_client=supabase_client,
        storage_adapter=storage_adapter,
        temporal_client=temporal_client,
    )

    assert result.queued_for_embedding == 1
    assert result.reused_storage_object == 1
    assert docs[0]["storage_path"] == "mounted://dof/5728067.doc"
    assert docs[0]["file_size_bytes"] == local_doc.stat().st_size
    assert docs[0]["embedding_status"] == "pending"


@pytest.mark.asyncio
async def test_backfill_source_falls_back_to_mounted_path_when_upload_times_out(tmp_path):
    (tmp_path / "dof").mkdir(parents=True)
    local_doc = tmp_path / "dof" / "5728068.doc"
    local_doc.write_bytes(b"slow legacy doc")

    docs = [
        {
            "id": "doc-1",
            "external_id": "5728068",
            "title": "Slow DOF document",
            "source_type": "dof",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-11T00:00:00+00:00",
        }
    ]

    supabase_client = _FakeSupabaseClient(docs)
    storage_adapter = _FakeStorageAdapter()
    storage_adapter.raise_on_doc_ids["5728068"] = TimeoutError("timed out")
    temporal_client = _FakeTemporalClient()

    result = await backfill_source_from_mounted_disk(
        "dof",
        limit=10,
        data_root=tmp_path,
        supabase_client=supabase_client,
        storage_adapter=storage_adapter,
        temporal_client=temporal_client,
    )

    assert result.queued_for_embedding == 1
    assert result.reused_storage_object == 1
    assert docs[0]["storage_path"] == "mounted://dof/5728068.doc"
    assert docs[0]["embedding_status"] == "pending"


@pytest.mark.asyncio
async def test_backfill_source_prefers_direct_mounted_path_for_bjv_without_upload_attempt(
    tmp_path,
    monkeypatch,
):
    monkeypatch.delenv("SCRAPER_DIRECT_MOUNTED_BACKFILL_SOURCES", raising=False)
    chapters_dir = tmp_path / "books" / "4312" / "chapters"
    chapters_dir.mkdir(parents=True)
    local_pdf = chapters_dir / "4_4312.pdf"
    local_pdf.write_bytes(b"bjv chapter pdf")

    docs = [
        {
            "id": "doc-1",
            "external_id": "book-4312-ch4",
            "title": "Mounted Biblio chapter",
            "publication_date": "2021-01-01",
            "source_type": "bjv",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-11T00:00:00+00:00",
        }
    ]

    supabase_client = _FakeSupabaseClient(docs)
    storage_adapter = _FakeStorageAdapter()
    temporal_client = _FakeTemporalClient()

    result = await backfill_source_from_mounted_disk(
        "bjv",
        limit=10,
        data_root=tmp_path,
        supabase_client=supabase_client,
        storage_adapter=storage_adapter,
        temporal_client=temporal_client,
    )

    assert result.queued_for_embedding == 1
    assert result.reused_storage_object == 1
    assert storage_adapter.upload_calls == []
    assert docs[0]["storage_path"] == "mounted://books/4312/chapters/4_4312.pdf"


@pytest.mark.asyncio
async def test_backfill_source_retries_stale_awaiting_source_rows(tmp_path, monkeypatch):
    monkeypatch.setenv("SCRAPER_AWAITING_SOURCE_RETRY_SECONDS", "60")
    (tmp_path / "books" / "4312" / "chapters").mkdir(parents=True)
    (tmp_path / "books" / "4312" / "chapters" / "4_4312.pdf").write_bytes(b"bjv chapter pdf")

    docs = [
        {
            "id": "doc-1",
            "external_id": "book-4312-ch4",
            "title": "Mounted Biblio chapter",
            "publication_date": "2021-01-01",
            "source_type": "bjv",
            "storage_path": None,
            "embedding_status": "awaiting_source_file",
            "chunk_count": 0,
            "created_at": "2026-04-11T00:00:00+00:00",
            "updated_at": "2026-04-11T00:00:00+00:00",
        }
    ]

    supabase_client = _FakeSupabaseClient(docs)
    storage_adapter = _FakeStorageAdapter()
    temporal_client = _FakeTemporalClient()

    result = await backfill_source_from_mounted_disk(
        "bjv",
        limit=10,
        data_root=tmp_path,
        supabase_client=supabase_client,
        storage_adapter=storage_adapter,
        temporal_client=temporal_client,
    )

    assert result.queued_for_embedding == 1
    assert docs[0]["storage_path"] == "mounted://books/4312/chapters/4_4312.pdf"
    assert docs[0]["embedding_status"] == "pending"


@pytest.mark.asyncio
async def test_backfill_source_defers_recent_awaiting_source_rows(tmp_path, monkeypatch):
    monkeypatch.setenv("SCRAPER_AWAITING_SOURCE_RETRY_SECONDS", "3600")
    (tmp_path / "books" / "4312" / "chapters").mkdir(parents=True)
    (tmp_path / "books" / "4312" / "chapters" / "4_4312.pdf").write_bytes(b"bjv chapter pdf")

    docs = [
        {
            "id": "doc-1",
            "external_id": "book-4312-ch4",
            "title": "Mounted Biblio chapter",
            "publication_date": "2021-01-01",
            "source_type": "bjv",
            "storage_path": None,
            "embedding_status": "awaiting_source_file",
            "chunk_count": 0,
            "created_at": "2026-04-11T00:00:00+00:00",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    ]

    supabase_client = _FakeSupabaseClient(docs)
    storage_adapter = _FakeStorageAdapter()
    temporal_client = _FakeTemporalClient()

    result = await backfill_source_from_mounted_disk(
        "bjv",
        limit=10,
        data_root=tmp_path,
        bootstrap_missing_documents=False,
        supabase_client=supabase_client,
        storage_adapter=storage_adapter,
        temporal_client=temporal_client,
    )

    assert result.queued_for_embedding == 0
    assert docs[0]["storage_path"] is None
    assert docs[0]["embedding_status"] == "awaiting_source_file"


@pytest.mark.asyncio
async def test_backfill_until_exhausted_stops_when_missing_storage_is_drained(tmp_path):
    (tmp_path / "dof" / "converted" / "5784621").mkdir(parents=True)
    (tmp_path / "dof" / "converted" / "5784621" / "output.txt").write_text(
        "first",
        encoding="utf-8",
    )
    (tmp_path / "dof" / "converted" / "5784622").mkdir(parents=True)
    (tmp_path / "dof" / "converted" / "5784622" / "output.txt").write_text(
        "second",
        encoding="utf-8",
    )

    docs = [
        {
            "id": "doc-1",
            "external_id": "5784621",
            "title": "First",
            "source_type": "dof",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-11T00:00:00+00:00",
        },
        {
            "id": "doc-2",
            "external_id": "5784622",
            "title": "Second",
            "source_type": "dof",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-10T00:00:00+00:00",
        },
    ]

    supabase_client = _FakeSupabaseClient(docs)
    storage_adapter = _FakeStorageAdapter()
    temporal_client = _FakeTemporalClient()

    result = await backfill_source_from_mounted_disk_until_exhausted(
        "dof",
        limit=1,
        include_existing_storage=False,
        data_root=tmp_path,
        supabase_client=supabase_client,
        storage_adapter=storage_adapter,
        temporal_client=temporal_client,
    )

    assert result.completed is True
    assert result.stalled is False
    assert result.passes == 2
    assert result.hydrated_documents == 2
    assert result.remaining_missing_storage == 0


@pytest.mark.asyncio
async def test_backfill_until_exhausted_continues_when_registration_progresses(monkeypatch, tmp_path):
    count_values = iter([1, 2, 2, 1, 1, 0])
    bootstrap_flags = []

    def fake_count(_client, _source):
        return next(count_values)

    async def fake_backfill(
        source,
        *,
        limit,
        include_existing_storage,
        bootstrap_missing_documents,
        trigger_embedding,
        wait_for_workflow,
        data_root,
        supabase_client,
        storage_adapter,
        temporal_client,
    ):
        bootstrap_flags.append(bootstrap_missing_documents)
        if len(bootstrap_flags) == 1:
            return mounted_disk_backfill.MountedBackfillResult(
                source=source,
                registered_rows=1,
            )
        if len(bootstrap_flags) == 2:
            return mounted_disk_backfill.MountedBackfillResult(
                source=source,
                queued_for_embedding=1,
            )
        return mounted_disk_backfill.MountedBackfillResult(source=source)

    monkeypatch.setattr(mounted_disk_backfill, "_count_documents_missing_storage", fake_count)
    monkeypatch.setattr(mounted_disk_backfill, "backfill_source_from_mounted_disk", fake_backfill)

    result = await backfill_source_from_mounted_disk_until_exhausted(
        "scjn",
        limit=10,
        data_root=tmp_path,
        supabase_client=_FakeSupabaseClient([]),
        storage_adapter=_FakeStorageAdapter(),
        temporal_client=_FakeTemporalClient(),
    )

    assert bootstrap_flags == [True, True, True]
    assert result.passes == 3
    assert result.completed is True
    assert result.stalled is False
    assert result.hydrated_documents == 2


def test_count_documents_missing_storage_uses_exact_count_without_paging():
    docs = [
        {
            "id": "doc-1",
            "external_id": "book-1",
            "title": "First",
            "source_type": "bjv",
            "storage_path": None,
        },
        {
            "id": "doc-2",
            "external_id": "book-2",
            "title": "Second",
            "source_type": "bjv",
            "storage_path": None,
        },
        {
            "id": "doc-3",
            "external_id": "book-3",
            "title": "Third",
            "source_type": "bjv",
            "storage_path": "bjv/ab/book-3.pdf",
        },
    ]

    supabase_client = _FakeSupabaseClient(docs)

    result = _count_documents_missing_storage(supabase_client, "bjv")

    assert result == 2
    assert len(supabase_client.queries) == 1
    assert supabase_client.queries[0].range_calls == 0


def test_resolve_local_source_file_matches_legacy_dof_slug_via_progress_db(tmp_path):
    db_path = tmp_path / "dof_progress.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE documents ("
        "id TEXT PRIMARY KEY, external_id TEXT UNIQUE NOT NULL, title TEXT, publication_date TEXT)"
    )
    conn.execute(
        "INSERT INTO documents (id, external_id, title, publication_date) VALUES (?, ?, ?, ?)",
        (
            "row-1",
            "5784621",
            "Tasas de interes interbancarias de equilibrio.",
            "2021-03-03",
        ),
    )
    conn.commit()
    conn.close()

    (tmp_path / "dof" / "converted" / "5784621").mkdir(parents=True)
    (tmp_path / "dof" / "converted" / "5784621" / "output.txt").write_text(
        "dof convertido",
        encoding="utf-8",
    )

    dof = resolve_local_source_file(
        "dof",
        "dof-Tasas_de_interes_interbancarias_de_equilibrio.",
        title="Tasas de interés interbancarias de equilibrio.",
        publication_date="2021-03-03",
        data_root=tmp_path,
    )

    assert dof is not None
    assert dof.path.name == "output.txt"


@pytest.mark.asyncio
async def test_backfill_source_prefers_direct_mounted_path_for_large_files_without_upload_attempt(
    tmp_path,
    monkeypatch,
):
    (tmp_path / "dof").mkdir(parents=True)
    local_doc = tmp_path / "dof" / "5728067.doc"
    local_doc.write_bytes(b"mounted-large-doc")
    monkeypatch.setenv("SCRAPER_STANDARD_UPLOAD_MAX_BYTES", "1")

    docs = [
        {
            "id": "doc-1",
            "external_id": "5728067",
            "title": "Large DOF document",
            "publication_date": "2021-03-03",
            "source_type": "dof",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-11T00:00:00+00:00",
        }
    ]

    supabase_client = _FakeSupabaseClient(docs)
    storage_adapter = _FakeStorageAdapter()
    temporal_client = _FakeTemporalClient()

    result = await backfill_source_from_mounted_disk(
        "dof",
        limit=10,
        data_root=tmp_path,
        supabase_client=supabase_client,
        storage_adapter=storage_adapter,
        temporal_client=temporal_client,
    )

    assert result.queued_for_embedding == 1
    assert result.reused_storage_object == 1
    assert storage_adapter.upload_calls == []
    assert docs[0]["storage_path"] == "mounted://dof/5728067.doc"


@pytest.mark.asyncio
async def test_backfill_source_uses_direct_mounted_path_for_cas_canonical_json(tmp_path):
    converted_dir = tmp_path / "cas" / "converted" / "A2-99"
    converted_dir.mkdir(parents=True)
    (converted_dir / "canonical.json").write_text(
        '{"sections":[{"text":"Appeal reasoning","type":"paragraph","heading_path":["Reasons"],"blocks":[{"page":1}]}]}',
        encoding="utf-8",
    )

    docs = [
        {
            "id": "doc-1",
            "external_id": "0000092c-0000-0000-0000-000000000000",
            "title": "CAS A2/99 AOC v. E. et al. — Boxing, Doping",
            "publication_date": "1999-01-01",
            "source_type": "cas",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
            "created_at": "2026-04-11T00:00:00+00:00",
        }
    ]

    supabase_client = _FakeSupabaseClient(docs)
    storage_adapter = _FakeStorageAdapter()
    temporal_client = _FakeTemporalClient()

    result = await backfill_source_from_mounted_disk(
        "cas",
        limit=10,
        data_root=tmp_path,
        supabase_client=supabase_client,
        storage_adapter=storage_adapter,
        temporal_client=temporal_client,
    )

    assert result.queued_for_embedding == 1
    assert result.reused_storage_object == 1
    assert storage_adapter.upload_calls == []
    assert docs[0]["storage_path"] == "mounted://cas/converted/A2-99/canonical.json"
