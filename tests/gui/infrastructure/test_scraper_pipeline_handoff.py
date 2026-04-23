from contextlib import nullcontext
from datetime import date
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

if "temporalio" not in sys.modules:
    def _identity_decorator(obj=None, *args, **kwargs):
        if obj is not None:
            return obj

        def _wrap(inner):
            return inner

        return _wrap

    temporalio_stub = SimpleNamespace(
        activity=SimpleNamespace(
            defn=_identity_decorator,
            logger=SimpleNamespace(
                info=lambda *args, **kwargs: None,
                warning=lambda *args, **kwargs: None,
                error=lambda *args, **kwargs: None,
            ),
        ),
        workflow=SimpleNamespace(
            defn=_identity_decorator,
            run=_identity_decorator,
            unsafe=SimpleNamespace(imports_passed_through=lambda: nullcontext()),
            logger=SimpleNamespace(
                info=lambda *args, **kwargs: None,
                warning=lambda *args, **kwargs: None,
                error=lambda *args, **kwargs: None,
            ),
        ),
    )
    temporalio_client_stub = SimpleNamespace(
        Client=object,
        Schedule=object,
        ScheduleActionStartWorkflow=object,
        ScheduleSpec=object,
    )
    temporalio_exceptions_stub = SimpleNamespace(ApplicationError=Exception)
    sys.modules["temporalio"] = temporalio_stub
    sys.modules["temporalio.common"] = SimpleNamespace(RetryPolicy=object)
    sys.modules["temporalio.client"] = temporalio_client_stub
    sys.modules["temporalio.exceptions"] = temporalio_exceptions_stub

import src.gui.infrastructure.scraper_document_workflow as scraper_document_workflow
import src.gui.infrastructure.crawl4ai_activities as crawl4ai_activities
from src.gui.infrastructure.crawl4ai_workflow import _resolve_embedding_document_ids
from src.gui.infrastructure.crawl4ai_activities import persist_documents_to_supabase
from src.gui.infrastructure.scraper_document_workflow import (
    fetch_documents_for_embedding,
    generate_and_store_embeddings,
)
from src.gui.infrastructure.temporal_client import TemporalConfig, TemporalScraperClient


class _PersistTable:
    def __init__(self, client):
        self._client = client
        self._row = None
        self._filters = {}
        self._limit = None
        self._select_mode = False

    def select(self, _columns):
        self._select_mode = True
        return self

    def eq(self, key, value):
        self._filters[key] = value
        return self

    def limit(self, value):
        self._limit = value
        return self

    def upsert(self, row, on_conflict=None):
        self._row = row
        return self

    def execute(self):
        if self._select_mode:
            data = [
                row for row in self._client.rows
                if all(row.get(key) == value for key, value in self._filters.items())
            ]
            if self._limit is not None:
                data = data[:self._limit]
            return SimpleNamespace(data=data)

        row = dict(self._row)
        existing = next(
            (
                item for item in self._client.rows
                if item.get("source_type") == row.get("source_type")
                and item.get("external_id") == row.get("external_id")
            ),
            None,
        )
        if existing is not None:
            existing.update(row)
            return SimpleNamespace(data=[{"id": existing["id"]}])

        next_id = f"uuid-{self._client.next_id}"
        self._client.next_id += 1
        row.setdefault("id", next_id)
        self._client.rows.append(row)
        return SimpleNamespace(data=[{"id": row["id"]}])


class _PersistClient:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.next_id = len(self.rows) + 1

    def table(self, name):
        assert name == "scraper_documents"
        return _PersistTable(self)


class _DocsQuery:
    def __init__(self, docs, observer=None):
        self._docs = list(docs)
        self._observer = observer
        self._ids = []
        self._limit = None
        self._range = None
        self._eq_filters = {}
        self._update_data = None

    def select(self, _columns):
        return self

    def in_(self, key, values):
        assert key == "id"
        self._ids = list(values)
        if self._observer is not None:
            self._observer.requested_id_batches.append(list(values))
        return self

    def eq(self, key, value):
        self._eq_filters[key] = value
        return self

    def order(self, *_args, **_kwargs):
        return self

    def range(self, start, end):
        self._range = (start, end)
        return self

    def limit(self, value):
        self._limit = value
        return self

    def update(self, values):
        self._update_data = values
        return self

    def execute(self):
        data = self._docs
        if self._ids:
            data = [doc for doc in data if doc["id"] in self._ids]
        for key, value in self._eq_filters.items():
            data = [doc for doc in data if doc.get(key) == value]
        if self._update_data is not None:
            for doc in data:
                doc.update(self._update_data)
            return SimpleNamespace(data=data)
        if self._range is not None:
            start, end = self._range
            data = data[start : end + 1]
        if self._limit is not None:
            data = data[:self._limit]
        return SimpleNamespace(data=data)


class _ChunksTable:
    def __init__(self, client):
        self._client = client
        self._ids = []
        self._eq_filters = {}
        self._delete_mode = False

    def select(self, _columns):
        return self

    def in_(self, key, values):
        assert key == "document_id"
        self._ids = list(values)
        return self

    def eq(self, key, value):
        self._eq_filters[key] = value
        return self

    def delete(self):
        self._delete_mode = True
        return self

    def upsert(self, rows, on_conflict=None):
        self._rows = rows
        return self

    def execute(self):
        if hasattr(self, "_rows"):
            rows = self._rows if isinstance(self._rows, list) else [self._rows]
            self._client.chunk_rows.extend(rows)
            return SimpleNamespace(data=self._rows)

        data = list(self._client.chunk_rows)
        if self._ids:
            data = [row for row in data if row.get("document_id") in self._ids]
        for key, value in self._eq_filters.items():
            data = [row for row in data if row.get(key) == value]

        if self._delete_mode:
            self._client.chunk_rows = [row for row in self._client.chunk_rows if row not in data]
            return SimpleNamespace(data=data)

        return SimpleNamespace(data=data)


class _FetchClient:
    def __init__(self, docs, existing_ids):
        self._docs = docs
        self.requested_id_batches = []
        self.chunk_rows = [
            {
                "chunk_id": f"existing-{doc_id}",
                "document_id": doc_id,
                "source_type": docs[0]["source_type"] if docs else "dof",
            }
            for doc_id in (existing_ids or set())
        ]

    def table(self, name):
        if name == "scraper_documents":
            return _DocsQuery(self._docs, observer=self)
        if name == "scraper_chunks":
            return _ChunksTable(self)
        raise AssertionError(name)


class _FakeClientSession:
    def __init__(self):
        self.payloads = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def post(self, url, headers=None, json=None):
        self.payloads.append(json)

        class _Response:
            status = 200

            async def __aenter__(self_inner):
                return self_inner

            async def __aexit__(self_inner, exc_type, exc, tb):
                return None

            async def json(self_inner):
                inputs = json["input"]
                return {
                    "data": [
                        {"embedding": [float(index + 1), 0.0, 0.0, 0.0]}
                        for index, _ in enumerate(inputs)
                    ]
                }

        return _Response()


@pytest.mark.asyncio
async def test_persist_documents_to_supabase_returns_document_ids(monkeypatch):
    fake_client = _PersistClient()
    monkeypatch.setenv("ENABLE_SUPABASE_PERSISTENCE", "true")
    monkeypatch.setenv("SUPABASE_URL", "http://example.test")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "secret")
    monkeypatch.setitem(
        sys.modules,
        "supabase",
        SimpleNamespace(create_client=lambda url, key: fake_client),
    )

    result = await persist_documents_to_supabase(
        [
            {
                "external_id": "ext-1",
                "title": "Doc 1",
                "publication_date": "2026-04-10",
                "pdf_storage_path": "legal-scraper-pdfs/dof/aa/ext-1.pdf",
            },
            {"external_id": "ext-2", "title": "Doc 2", "publication_date": "2026-04-11"},
        ],
        "dof",
    )

    assert result["success"] is True
    assert result["persisted_count"] == 2
    assert result["document_ids"] == ["uuid-1", "uuid-2"]
    assert fake_client.rows[0]["storage_path"] == "legal-scraper-pdfs/dof/aa/ext-1.pdf"


@pytest.mark.asyncio
async def test_persist_documents_to_supabase_preserves_storage_and_restores_pending(monkeypatch):
    fake_client = _PersistClient(
        rows=[
            {
                "id": "uuid-1",
                "source_type": "dof",
                "external_id": "ext-1",
                "title": "Old Doc 1",
                "publication_date": "2026-04-01",
                "storage_path": "legal-scraper-pdfs/dof/existing/ext-1.pdf",
                "embedding_status": "completed",
                "chunk_count": 4,
            },
            {
                "id": "uuid-2",
                "source_type": "dof",
                "external_id": "ext-2",
                "title": "Old Doc 2",
                "publication_date": "2026-04-01",
                "storage_path": None,
                "embedding_status": "pending",
                "chunk_count": 0,
            },
        ]
    )
    monkeypatch.setenv("ENABLE_SUPABASE_PERSISTENCE", "true")
    monkeypatch.setenv("SUPABASE_URL", "http://example.test")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "secret")
    monkeypatch.setitem(
        sys.modules,
        "supabase",
        SimpleNamespace(create_client=lambda url, key: fake_client),
    )

    result = await persist_documents_to_supabase(
        [
            {
                "external_id": "ext-1",
                "title": "Doc 1 Refresh",
                "publication_date": "2026-04-10T12:00:00Z",
            },
            {
                "external_id": "ext-2",
                "title": "Doc 2 Refresh",
                "publication_date": "2026-04-11",
                "pdf_storage_path": "legal-scraper-pdfs/dof/new/ext-2.pdf",
            },
        ],
        "dof",
    )

    assert result["success"] is True
    assert result["document_ids"] == ["uuid-1", "uuid-2"]
    assert fake_client.rows[0]["storage_path"] == "legal-scraper-pdfs/dof/existing/ext-1.pdf"
    assert fake_client.rows[0]["embedding_status"] == "completed"
    assert fake_client.rows[1]["storage_path"] == "legal-scraper-pdfs/dof/new/ext-2.pdf"
    assert fake_client.rows[1]["embedding_status"] == "pending"


@pytest.mark.asyncio
async def test_temporal_client_passes_document_ids_to_workflow():
    client = TemporalScraperClient(
        TemporalConfig(
            address="temporal:7233",
            namespace="default",
            task_queue="scraper-pipeline",
            enabled=True,
        )
    )
    mock_temporal = MagicMock()
    mock_temporal.start_workflow = AsyncMock(
        return_value=SimpleNamespace(result_run_id="run-123")
    )
    client._get_client = AsyncMock(return_value=mock_temporal)

    run_id = await client.trigger_embedding_workflow(
        job_id="job-1",
        source_type="dof",
        document_count=3,
        document_ids=["doc-1", "doc-2", "doc-2"],
        batch_size=25,
    )

    assert run_id == "run-123"
    workflow_input = mock_temporal.start_workflow.await_args.args[1]
    assert workflow_input["document_ids"] == ["doc-1", "doc-2"]
    assert workflow_input["max_documents"] == 2
    assert workflow_input["batch_size"] == 25


@pytest.mark.asyncio
async def test_temporal_client_skips_embedding_without_document_ids():
    client = TemporalScraperClient(
        TemporalConfig(
            address="temporal:7233",
            namespace="default",
            task_queue="scraper-pipeline",
            enabled=True,
        )
    )
    client._get_client = AsyncMock()

    run_id = await client.trigger_embedding_workflow(
        job_id="job-1",
        source_type="dof",
        document_count=3,
        document_ids=[],
    )

    assert run_id is None
    client._get_client.assert_not_awaited()


@pytest.mark.asyncio
async def test_fetch_documents_for_embedding_skips_requested_completed_docs(monkeypatch):
    docs = [
        {
            "id": "doc-1",
            "title": "One",
            "external_id": "ext-1",
            "source_type": "dof",
            "storage_path": "legal-scraper-pdfs/dof/aa/ext-1.pdf",
            "embedding_status": "pending",
            "chunk_count": 0,
        },
        {
            "id": "doc-2",
            "title": "Two",
            "external_id": "ext-2",
            "source_type": "dof",
            "storage_path": "legal-scraper-pdfs/dof/bb/ext-2.pdf",
            "embedding_status": "completed",
            "chunk_count": 4,
        },
    ]
    fake_client = _FetchClient(docs=docs, existing_ids={"doc-2"})
    monkeypatch.setenv("SUPABASE_URL", "http://example.test")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "secret")
    monkeypatch.setitem(
        sys.modules,
        "supabase",
        SimpleNamespace(create_client=lambda url, key: fake_client),
    )

    result = await fetch_documents_for_embedding("dof", ["doc-1", "doc-2"], 10)

    assert [doc["id"] for doc in result] == ["doc-1"]
    assert result[0]["storage_path"] == "legal-scraper-pdfs/dof/aa/ext-1.pdf"


@pytest.mark.asyncio
async def test_fetch_documents_for_embedding_retries_partial_requested_docs(monkeypatch):
    docs = [
        {
            "id": "doc-1",
            "title": "One",
            "external_id": "ext-1",
            "source_type": "dof",
            "storage_path": "legal-scraper-pdfs/dof/aa/ext-1.pdf",
            "embedding_status": "processing",
            "chunk_count": 1,
        }
    ]
    fake_client = _FetchClient(docs=docs, existing_ids={"doc-1"})
    monkeypatch.setenv("SUPABASE_URL", "http://example.test")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "secret")
    monkeypatch.setitem(
        sys.modules,
        "supabase",
        SimpleNamespace(create_client=lambda url, key: fake_client),
    )

    result = await fetch_documents_for_embedding("dof", ["doc-1"], 10)

    assert [doc["id"] for doc in result] == ["doc-1"]


@pytest.mark.asyncio
async def test_fetch_documents_for_embedding_marks_missing_storage_docs(monkeypatch):
    docs = [
        {
            "id": "doc-1",
            "title": "Missing File",
            "external_id": "ext-1",
            "source_type": "dof",
            "storage_path": None,
            "embedding_status": "pending",
            "chunk_count": 0,
        },
        {
            "id": "doc-2",
            "title": "Ready",
            "external_id": "ext-2",
            "source_type": "dof",
            "storage_path": "legal-scraper-pdfs/dof/bb/ext-2.pdf",
            "embedding_status": "pending",
            "chunk_count": 0,
        },
    ]
    fake_client = _FetchClient(docs=docs, existing_ids=set())
    monkeypatch.setenv("SUPABASE_URL", "http://example.test")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "secret")
    monkeypatch.setitem(
        sys.modules,
        "supabase",
        SimpleNamespace(create_client=lambda url, key: fake_client),
    )

    result = await fetch_documents_for_embedding("dof", [], 10)

    assert [doc["id"] for doc in result] == ["doc-2"]
    assert fake_client._docs[0]["embedding_status"] == "pending"


@pytest.mark.asyncio
async def test_fetch_documents_for_embedding_requeues_stale_processing_docs(monkeypatch):
    docs = [
        {
            "id": "doc-1",
            "title": "Stale",
            "external_id": "ext-1",
            "source_type": "dof",
            "storage_path": "legal-scraper-pdfs/dof/aa/ext-1.pdf",
            "embedding_status": "processing",
            "chunk_count": 0,
            "updated_at": "2020-01-01T00:00:00+00:00",
        },
        {
            "id": "doc-2",
            "title": "Fresh",
            "external_id": "ext-2",
            "source_type": "dof",
            "storage_path": "legal-scraper-pdfs/dof/bb/ext-2.pdf",
            "embedding_status": "processing",
            "chunk_count": 0,
            "updated_at": "2999-01-01T00:00:00+00:00",
        },
        {
            "id": "doc-3",
            "title": "Pending",
            "external_id": "ext-3",
            "source_type": "dof",
            "storage_path": "legal-scraper-pdfs/dof/cc/ext-3.pdf",
            "embedding_status": "pending",
            "chunk_count": 0,
            "updated_at": "2026-04-12T00:00:00+00:00",
        },
    ]
    fake_client = _FetchClient(docs=docs, existing_ids=set())
    monkeypatch.setenv("SUPABASE_URL", "http://example.test")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "secret")
    monkeypatch.setenv("SCRAPER_EMBEDDING_PROCESSING_STALE_HOURS", "24")
    monkeypatch.setitem(
        sys.modules,
        "supabase",
        SimpleNamespace(create_client=lambda url, key: fake_client),
    )

    result = await fetch_documents_for_embedding("dof", [], 10)

    assert [doc["id"] for doc in result] == ["doc-1", "doc-3"]
    assert fake_client._docs[0]["embedding_status"] == "pending"
    assert fake_client._docs[1]["embedding_status"] == "processing"


def test_safe_update_document_state_stamps_updated_at():
    fake_client = _FetchClient(
        docs=[
            {
                "id": "doc-1",
                "title": "One",
                "external_id": "ext-1",
                "source_type": "dof",
                "storage_path": "legal-scraper-pdfs/dof/aa/ext-1.pdf",
                "embedding_status": "pending",
                "chunk_count": 0,
            }
        ],
        existing_ids=set(),
    )

    scraper_document_workflow._safe_update_document_state(
        fake_client,
        "doc-1",
        embedding_status="processing",
    )

    assert fake_client._docs[0]["embedding_status"] == "processing"
    assert "updated_at" in fake_client._docs[0]
    assert "processing_started_at" in fake_client._docs[0]
    assert fake_client._docs[0]["processed_at"] is None


def test_safe_update_document_state_sets_processed_at_on_completion():
    fake_client = _FetchClient(
        docs=[
            {
                "id": "doc-1",
                "title": "One",
                "external_id": "ext-1",
                "source_type": "dof",
                "storage_path": "legal-scraper-pdfs/dof/aa/ext-1.pdf",
                "embedding_status": "processing",
                "chunk_count": 0,
                "processing_started_at": "2026-04-12T00:00:00+00:00",
            }
        ],
        existing_ids=set(),
    )

    scraper_document_workflow._safe_update_document_state(
        fake_client,
        "doc-1",
        embedding_status="completed",
        chunk_count=3,
    )

    assert fake_client._docs[0]["embedding_status"] == "completed"
    assert fake_client._docs[0]["chunk_count"] == 3
    assert "processed_at" in fake_client._docs[0]


@pytest.mark.asyncio
async def test_fetch_documents_for_embedding_batches_large_requested_id_sets(monkeypatch):
    docs = [
        {
            "id": f"doc-{index}",
            "title": f"Doc {index}",
            "external_id": f"ext-{index}",
            "source_type": "dof",
            "storage_path": f"legal-scraper-pdfs/dof/{index:02d}/ext-{index}.pdf",
            "embedding_status": "pending",
            "chunk_count": 0,
        }
        for index in range(1, 206)
    ]
    requested_ids = [doc["id"] for doc in docs]
    fake_client = _FetchClient(docs=docs, existing_ids=set())
    monkeypatch.setenv("SUPABASE_URL", "http://example.test")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "secret")
    monkeypatch.setitem(
        sys.modules,
        "supabase",
        SimpleNamespace(create_client=lambda url, key: fake_client),
    )

    result = await fetch_documents_for_embedding("dof", requested_ids, 205)

    assert len(result) == 205
    assert len(fake_client.requested_id_batches) == 3
    assert [len(batch) for batch in fake_client.requested_id_batches] == [100, 100, 5]


def test_resolve_embedding_document_ids_requires_persisted_ids():
    document_ids, errors = _resolve_embedding_document_ids(
        trigger_embedding=True,
        document_count=3,
        persistence_result={"success": True, "document_ids": []},
    )

    assert document_ids == []
    assert len(errors) == 1
    assert "no document IDs" in errors[0]


def test_resolve_embedding_document_ids_uses_unique_persisted_ids():
    document_ids, errors = _resolve_embedding_document_ids(
        trigger_embedding=True,
        document_count=3,
        persistence_result={"success": True, "document_ids": ["doc-1", "doc-2", "doc-1"]},
    )

    assert document_ids == ["doc-1", "doc-2"]
    assert errors == []


def test_chunk_unstructured_elements_tracks_heading_path():
    Title = type(
        "Title",
        (),
        {
            "__init__": lambda self, text, page=None, depth=None: setattr(
                self,
                "metadata",
                SimpleNamespace(page_number=page, category_depth=depth),
            )
            or setattr(self, "_text", text),
            "__str__": lambda self: self._text,
        },
    )
    NarrativeText = type(
        "NarrativeText",
        (),
        {
            "__init__": lambda self, text, page=None: setattr(
                self,
                "metadata",
                SimpleNamespace(page_number=page),
            )
            or setattr(self, "_text", text),
            "__str__": lambda self: self._text,
        },
    )

    chunks = scraper_document_workflow._chunk_unstructured_elements(
        [
            Title("Main Heading", page=1, depth=0),
            Title("Sub Heading", page=1, depth=1),
            NarrativeText("Body paragraph.", page=2),
        ],
        "Fallback Title",
    )

    assert len(chunks) == 1
    assert chunks[0]["title"] == "Sub Heading"
    assert chunks[0]["metadata"]["heading"] == "Sub Heading"
    assert chunks[0]["metadata"]["heading_path"] == ["Main Heading", "Sub Heading"]
    assert chunks[0]["metadata"]["heading_level"] == 2
    assert chunks[0]["metadata"]["page_start"] == 2
    assert "Body paragraph." in chunks[0]["text"]


@pytest.mark.asyncio
async def test_generate_and_store_embeddings_uses_unstructured_chunks(monkeypatch):
    fake_client = _FetchClient(
        docs=[
            {
                "id": "doc-1",
                "title": "Doc One",
                "external_id": "ext-1",
                "source_type": "dof",
                "storage_path": "legal-scraper-pdfs/dof/aa/ext-1.pdf",
                "embedding_status": "pending",
                "chunk_count": 0,
            }
        ],
        existing_ids=None,
    )
    fake_client.chunk_rows.append(
        {
            "chunk_id": "stale-doc-1",
            "document_id": "doc-1",
            "source_type": "dof",
            "chunk_index": 99,
            "text": "old",
        }
    )
    monkeypatch.setenv("SUPABASE_URL", "http://example.test")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "secret")
    monkeypatch.setenv("OPENROUTER_API_KEY", "router-secret")
    monkeypatch.setenv("SCRAPER_PDF_BUCKET", "legal-scraper-pdfs")
    monkeypatch.setitem(
        sys.modules,
        "supabase",
        SimpleNamespace(create_client=lambda url, key: fake_client),
    )
    monkeypatch.setitem(
        sys.modules,
        "src.infrastructure.adapters.supabase_storage",
        SimpleNamespace(
            SupabaseStorageAdapter=lambda client, bucket_name: SimpleNamespace(
                download=AsyncMock(return_value=b"%PDF-1.4 fake")
            )
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "aiohttp",
        SimpleNamespace(ClientSession=lambda: _FakeClientSession()),
    )
    monkeypatch.setattr(
        scraper_document_workflow,
        "_extract_document_chunks",
        lambda file_path, fallback_title: [
            {
                "text": "Chunk 1 text",
                "title": "Section I",
                "metadata": {
                    "page_start": 1,
                    "page_end": 1,
                    "element_types": ["NarrativeText"],
                    "token_count": 12,
                },
            },
            {
                "text": "Chunk 2 text",
                "title": "Section II",
                "metadata": {
                    "page_start": 2,
                    "page_end": 2,
                    "element_types": ["NarrativeText"],
                    "token_count": 9,
                },
            },
        ],
    )

    result = await generate_and_store_embeddings(
        [
            {
                "id": "doc-1",
                "title": "Doc One",
                "external_id": "ext-1",
                "storage_path": "legal-scraper-pdfs/dof/aa/ext-1.pdf",
            }
        ],
        "dof",
    )

    assert result["stored_count"] == 2
    assert result["stored_documents"] == 1
    assert result["error_count"] == 0
    assert [row["chunk_id"] for row in fake_client.chunk_rows] == [
        "dof-doc-1-0",
        "dof-doc-1-1",
    ]
    assert fake_client.chunk_rows[0]["titulo"] == "Section I"
    assert fake_client.chunk_rows[0]["token_count"] == 12
    assert (
        fake_client.chunk_rows[0]["metadata"]["storage_path"]
        == "legal-scraper-pdfs/dof/aa/ext-1.pdf"
    )
    docs = fake_client._docs
    assert docs[0]["embedding_status"] == "completed"
    assert docs[0]["chunk_count"] == 2


@pytest.mark.asyncio
async def test_generate_and_store_embeddings_reads_mounted_storage_path(monkeypatch, tmp_path):
    mounted_root = tmp_path / "mounted"
    mounted_doc = mounted_root / "dof" / "5728067.doc"
    mounted_doc.parent.mkdir(parents=True)
    mounted_doc.write_bytes(b"legacy mounted doc")

    fake_client = _FetchClient(
        docs=[
            {
                "id": "doc-1",
                "title": "Mounted Doc",
                "external_id": "5728067",
                "source_type": "dof",
                "storage_path": "mounted://dof/5728067.doc",
                "embedding_status": "pending",
                "chunk_count": 0,
            }
        ],
        existing_ids=None,
    )
    download_mock = AsyncMock(return_value=b"should not be used")
    monkeypatch.setenv("SUPABASE_URL", "http://example.test")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "secret")
    monkeypatch.setenv("OPENROUTER_API_KEY", "router-secret")
    monkeypatch.setenv("SCRAPER_PDF_BUCKET", "legal-scraper-pdfs")
    monkeypatch.setenv("SCRAPER_MOUNTED_DATA_ROOT", str(mounted_root))
    monkeypatch.setitem(
        sys.modules,
        "supabase",
        SimpleNamespace(create_client=lambda url, key: fake_client),
    )
    monkeypatch.setitem(
        sys.modules,
        "src.infrastructure.adapters.supabase_storage",
        SimpleNamespace(
            SupabaseStorageAdapter=lambda client, bucket_name: SimpleNamespace(
                download=download_mock
            )
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "aiohttp",
        SimpleNamespace(ClientSession=lambda: _FakeClientSession()),
    )
    monkeypatch.setattr(
        scraper_document_workflow,
        "_extract_document_chunks",
        lambda file_path, fallback_title: [
            {
                "text": f"Chunk from {file_path.name}",
                "title": fallback_title,
                "metadata": {
                    "page_start": 1,
                    "page_end": 1,
                    "element_types": ["NarrativeText"],
                    "token_count": 5,
                },
            }
        ],
    )

    result = await generate_and_store_embeddings(
        [
            {
                "id": "doc-1",
                "title": "Mounted Doc",
                "external_id": "5728067",
                "storage_path": "mounted://dof/5728067.doc",
            }
        ],
        "dof",
    )

    assert result["stored_documents"] == 1
    assert result["stored_count"] == 1
    download_mock.assert_not_awaited()
    assert fake_client.chunk_rows[0]["metadata"]["storage_path"] == "mounted://dof/5728067.doc"


@pytest.mark.asyncio
async def test_generate_and_store_embeddings_uses_cas_canonical_chunker(monkeypatch, tmp_path):
    mounted_root = tmp_path / "mounted"
    mounted_doc = mounted_root / "cas" / "converted" / "A2-99" / "canonical.json"
    mounted_doc.parent.mkdir(parents=True)
    mounted_doc.write_text('{"sections":[]}', encoding="utf-8")

    fake_client = _FetchClient(
        docs=[
            {
                "id": "doc-1",
                "title": "CAS Appeal",
                "external_id": "A2-99",
                "source_type": "cas",
                "storage_path": "mounted://cas/converted/A2-99/canonical.json",
                "embedding_status": "pending",
                "chunk_count": 0,
            }
        ],
        existing_ids=None,
    )
    download_mock = AsyncMock(return_value=b"should not be used")
    monkeypatch.setenv("SUPABASE_URL", "http://example.test")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "secret")
    monkeypatch.setenv("OPENROUTER_API_KEY", "router-secret")
    monkeypatch.setenv("SCRAPER_PDF_BUCKET", "legal-scraper-pdfs")
    monkeypatch.setenv("SCRAPER_MOUNTED_DATA_ROOT", str(mounted_root))
    monkeypatch.setitem(
        sys.modules,
        "supabase",
        SimpleNamespace(create_client=lambda url, key: fake_client),
    )
    monkeypatch.setitem(
        sys.modules,
        "src.infrastructure.adapters.supabase_storage",
        SimpleNamespace(
            SupabaseStorageAdapter=lambda client, bucket_name: SimpleNamespace(
                download=download_mock
            )
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "aiohttp",
        SimpleNamespace(ClientSession=lambda: _FakeClientSession()),
    )
    monkeypatch.setitem(
        sys.modules,
        "src.gui.infrastructure.cas_chunking",
        SimpleNamespace(
            is_cas_canonical_json=lambda path: True,
            extract_cas_canonical_chunks=lambda file_path, fallback_title: [
                {
                    "text": "Canonical CAS chunk",
                    "title": "Reasons",
                    "metadata": {
                        "heading": "Reasons",
                        "heading_path": ["Reasons"],
                        "heading_level": 1,
                        "page_start": 2,
                        "page_end": 3,
                        "section_types": ["paragraph"],
                        "chunking_method": "canonical_section_v1",
                        "token_count": 11,
                    },
                }
            ],
        ),
    )
    monkeypatch.setattr(
        scraper_document_workflow,
        "_extract_document_chunks",
        lambda file_path, fallback_title: (_ for _ in ()).throw(
            AssertionError("generic chunker should not run for canonical CAS files")
        ),
    )

    result = await generate_and_store_embeddings(
        [
            {
                "id": "doc-1",
                "title": "CAS Appeal",
                "external_id": "A2-99",
                "source_type": "cas",
                "storage_path": "mounted://cas/converted/A2-99/canonical.json",
            }
        ],
        "cas",
    )

    assert result["stored_documents"] == 1
    assert result["stored_count"] == 1
    assert fake_client.chunk_rows[0]["titulo"] == "Reasons"
    assert fake_client.chunk_rows[0]["metadata"]["chunking_method"] == "canonical_section_v1"
    download_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_generate_and_store_embeddings_uses_scjn_api_json_chunker(monkeypatch, tmp_path):
    mounted_root = tmp_path / "mounted"
    mounted_doc = mounted_root / "ejecutorias" / "api_json" / "33873.json"
    mounted_doc.parent.mkdir(parents=True)
    mounted_doc.write_text(
        '{"rubro":"<p>Ejecutoria de prueba</p>","texto":"<p>Texto principal</p>"}',
        encoding="utf-8",
    )

    fake_client = _FetchClient(
        docs=[
            {
                "id": "doc-1",
                "title": "Ejecutoria 33873",
                "external_id": "ejecutorias-33873",
                "source_type": "scjn",
                "storage_path": "mounted://ejecutorias/api_json/33873.json",
                "embedding_status": "pending",
                "chunk_count": 0,
            }
        ],
        existing_ids=None,
    )
    download_mock = AsyncMock(return_value=b"should not be used")
    monkeypatch.setenv("SUPABASE_URL", "http://example.test")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "secret")
    monkeypatch.setenv("OPENROUTER_API_KEY", "router-secret")
    monkeypatch.setenv("SCRAPER_PDF_BUCKET", "legal-scraper-pdfs")
    monkeypatch.setenv("SCRAPER_MOUNTED_DATA_ROOT", str(mounted_root))
    monkeypatch.setitem(
        sys.modules,
        "supabase",
        SimpleNamespace(create_client=lambda url, key: fake_client),
    )
    monkeypatch.setitem(
        sys.modules,
        "src.infrastructure.adapters.supabase_storage",
        SimpleNamespace(
            SupabaseStorageAdapter=lambda client, bucket_name: SimpleNamespace(
                download=download_mock
            )
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "aiohttp",
        SimpleNamespace(ClientSession=lambda: _FakeClientSession()),
    )
    monkeypatch.setattr(
        scraper_document_workflow,
        "_extract_document_chunks",
        lambda file_path, fallback_title: (_ for _ in ()).throw(
            AssertionError("generic chunker should not run for SCJN API JSON files")
        ),
    )

    result = await generate_and_store_embeddings(
        [
            {
                "id": "doc-1",
                "title": "Ejecutoria 33873",
                "external_id": "ejecutorias-33873",
                "source_type": "scjn",
                "storage_path": "mounted://ejecutorias/api_json/33873.json",
            }
        ],
        "scjn",
    )

    assert result["stored_documents"] == 1
    assert result["stored_count"] == len(fake_client.chunk_rows)
    assert len(fake_client.chunk_rows) >= 2
    assert all(
        row["metadata"]["chunking_method"] == "scjn_api_json_v1" for row in fake_client.chunk_rows
    )
    combined_text = "\n".join(row["text"] for row in fake_client.chunk_rows)
    assert "Ejecutoria de prueba" in combined_text
    assert "Texto principal" in combined_text
    download_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_generate_and_store_embeddings_uses_bjv_chunker(monkeypatch, tmp_path):
    mounted_root = tmp_path / "mounted"
    mounted_doc = mounted_root / "books" / "7896" / "chapters" / "1.pdf"
    mounted_doc.parent.mkdir(parents=True)
    mounted_doc.write_bytes(b"%PDF-1.4 fake")

    fake_client = _FetchClient(
        docs=[
            {
                "id": "doc-1",
                "title": "Capítulo 1",
                "external_id": "book-7896-ch1",
                "source_type": "bjv",
                "storage_path": "mounted://books/7896/chapters/1.pdf",
                "embedding_status": "pending",
                "chunk_count": 0,
            }
        ],
        existing_ids=None,
    )
    download_mock = AsyncMock(return_value=b"should not be used")
    monkeypatch.setenv("SUPABASE_URL", "http://example.test")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "secret")
    monkeypatch.setenv("OPENROUTER_API_KEY", "router-secret")
    monkeypatch.setenv("SCRAPER_PDF_BUCKET", "legal-scraper-pdfs")
    monkeypatch.setenv("SCRAPER_MOUNTED_DATA_ROOT", str(mounted_root))
    monkeypatch.setitem(
        sys.modules,
        "supabase",
        SimpleNamespace(create_client=lambda url, key: fake_client),
    )
    monkeypatch.setitem(
        sys.modules,
        "src.infrastructure.adapters.supabase_storage",
        SimpleNamespace(
            SupabaseStorageAdapter=lambda client, bucket_name: SimpleNamespace(
                download=download_mock
            )
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "aiohttp",
        SimpleNamespace(ClientSession=lambda: _FakeClientSession()),
    )
    monkeypatch.setitem(
        sys.modules,
        "src.gui.infrastructure.biblio_chunking",
        SimpleNamespace(
            extract_biblio_chunks=lambda file_path, fallback_title: [
                {
                    "text": "Biblio OCR chunk",
                    "title": "Capítulo I",
                    "metadata": {
                        "heading": "Capítulo I",
                        "heading_path": ["Libro", "Capítulo I"],
                        "heading_level": 2,
                        "page_start": 1,
                        "page_end": 2,
                        "chunking_method": "biblio_pymupdf_routed_heading_v1",
                        "token_count": 9,
                        "asset_role": "chapter",
                        "chapter_number": "1",
                    },
                }
            ],
        ),
    )
    monkeypatch.setattr(
        scraper_document_workflow,
        "_extract_document_chunks",
        lambda file_path, fallback_title: (_ for _ in ()).throw(
            AssertionError("generic chunker should not run for Biblio PDFs")
        ),
    )

    result = await generate_and_store_embeddings(
        [
            {
                "id": "doc-1",
                "title": "Capítulo 1",
                "external_id": "book-7896-ch1",
                "source_type": "bjv",
                "storage_path": "mounted://books/7896/chapters/1.pdf",
            }
        ],
        "bjv",
    )

    assert result["stored_documents"] == 1
    assert result["stored_count"] == 1
    assert fake_client.chunk_rows[0]["titulo"] == "Capítulo I"
    assert fake_client.chunk_rows[0]["metadata"]["chunking_method"] == "biblio_pymupdf_routed_heading_v1"
    download_mock.assert_not_awaited()


class _FakeHttpResponse:
    def __init__(self, body=b"", status=200, text=""):
        self._body = body
        self.status = status
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def read(self):
        return self._body

    async def text(self, encoding="utf-8", errors="replace"):
        return self._text


class _FakeHttpSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.requested_urls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def get(self, url, **kwargs):
        self.requested_urls.append(url)
        return self._responses.pop(0)


@pytest.mark.asyncio
async def test_extract_bjv_documents_resolves_pdf_url_from_detail_page(monkeypatch, tmp_path):
    fake_book = SimpleNamespace(
        libro_id=SimpleNamespace(bjv_id="123"),
        titulo="Libro de Prueba",
        tipo_contenido=SimpleNamespace(value="libro"),
        anio=2024,
        autores_texto="Autora",
        area="civil",
        detail_url="/bjv/detalle-libro/123-libro-de-prueba",
        url_pdf=None,
    )

    class _FakeBJVParser:
        async def search_multiple_pages(self, query, area=None, max_pages=5, max_results=None):
            return [fake_book]

    class _FakeBJVLibroParser:
        BASE_URL = "https://biblio.juridicas.unam.mx"

        def parse_libro_detalle(self, html, libro_id):
            assert libro_id == "123"
            return SimpleNamespace(url_pdf=SimpleNamespace(url="/pdfs/libro-prueba.pdf"))

    session = _FakeHttpSession([_FakeHttpResponse(text="<html></html>")])
    monkeypatch.setitem(
        sys.modules,
        "src.infrastructure.adapters.bjv_llm_parser",
        SimpleNamespace(BJVLLMParser=lambda: _FakeBJVParser()),
    )
    monkeypatch.setitem(
        sys.modules,
        "src.infrastructure.adapters.bjv_libro_parser",
        SimpleNamespace(BJVLibroParser=_FakeBJVLibroParser),
    )
    monkeypatch.setattr(
        crawl4ai_activities.aiohttp,
        "ClientSession",
        lambda *args, **kwargs: session,
    )

    result = await crawl4ai_activities.extract_bjv_documents(
        max_results=1,
        search_term="prueba",
        area="civil",
        output_directory=str(tmp_path / "bjv_data"),
    )

    assert result["success"] is True
    assert result["document_count"] == 1
    assert result["documents"][0]["url"] == "/bjv/detalle-libro/123-libro-de-prueba"
    assert (
        result["documents"][0]["pdf_url"]
        == "https://biblio.juridicas.unam.mx/pdfs/libro-prueba.pdf"
    )
    assert session.requested_urls == ["https://biblio.juridicas.unam.mx/bjv/detalle-libro/123-libro-de-prueba"]


@pytest.mark.asyncio
async def test_download_documents_pdfs_stores_bjv_detail_html_when_pdf_missing(monkeypatch, tmp_path):
    detail_html = """
    <html>
        <head>
            <meta property="dc:identifier" content="7439" />
            <title>Obras sobre derechos de los pueblos indígenas</title>
        </head>
        <body>
            <div class="col-md-12 titulo">Obras sobre derechos de los pueblos indígenas</div>
            <div class="contenido-indice-titulo">CONTENIDO</div>
            <p>Una página curada sin archivo PDF, pero con contenido canónico para indexar.</p>
        </body>
    </html>
    """

    session = _FakeHttpSession(
        [_FakeHttpResponse(body=detail_html.encode("utf-8"), text=detail_html)]
    )
    monkeypatch.setattr(
        crawl4ai_activities.aiohttp,
        "ClientSession",
        lambda *args, **kwargs: session,
    )
    monkeypatch.setattr(
        crawl4ai_activities,
        "_extract_bjv_pdf_url",
        AsyncMock(return_value=None),
    )

    result = await crawl4ai_activities.download_documents_pdfs(
        documents=[
            {
                "external_id": "7439",
                "url": "/bjv/detalle-libro/7439-obras-sobre-derechos-de-los-pueblos-indigenas",
                "pdf_url": None,
            }
        ],
        source="bjv",
        output_directory=str(tmp_path / "downloads"),
        upload_to_storage=False,
    )

    assert result["downloaded_count"] == 1
    assert result["failed_count"] == 0
    assert result["skipped_count"] == 0
    assert result["downloaded_documents"][0]["content_type"] == "text/html"
    assert result["downloaded_documents"][0]["pdf_path"].endswith(".html")
    assert session.requested_urls == [
        "https://biblio.juridicas.unam.mx/bjv/detalle-libro/7439-obras-sobre-derechos-de-los-pueblos-indigenas"
    ]


def test_extract_document_chunks_supports_html_source_files(monkeypatch, tmp_path):
    html_path = tmp_path / "source.html"
    html_path.write_text("<html><body><p>Texto canónico</p></body></html>", encoding="utf-8")

    calls = []

    def _fake_partition_html(filename):
        calls.append(filename)
        return ["Texto canónico"]

    monkeypatch.setitem(
        sys.modules,
        "unstructured.partition.html",
        SimpleNamespace(partition_html=_fake_partition_html),
    )

    chunks = scraper_document_workflow._extract_document_chunks(html_path, "Documento HTML")

    assert calls == [str(html_path)]
    assert len(chunks) == 1
    assert "Texto canónico" in chunks[0]["text"]


def test_extract_document_chunks_falls_back_when_pdf_inference_dependency_is_missing(
    monkeypatch, tmp_path
):
    pdf_path = tmp_path / "source.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 fake")

    def _fake_partition_pdf(filename, strategy):
        raise ModuleNotFoundError("No module named 'unstructured_inference'")

    def _fake_partition_text(text=None, filename=None):
        assert text == "Primera página\n\nSegunda página"
        return ["Texto derivado de PDF"]

    class _FakePdfReader:
        def __init__(self, filename):
            self.pages = [
                SimpleNamespace(extract_text=lambda: "Primera página"),
                SimpleNamespace(extract_text=lambda: "Segunda página"),
            ]

    monkeypatch.setitem(
        sys.modules,
        "unstructured.partition.pdf",
        SimpleNamespace(partition_pdf=_fake_partition_pdf),
    )
    monkeypatch.setitem(
        sys.modules,
        "unstructured.partition.text",
        SimpleNamespace(partition_text=_fake_partition_text),
    )
    monkeypatch.setitem(sys.modules, "pypdf", SimpleNamespace(PdfReader=_FakePdfReader))

    chunks = scraper_document_workflow._extract_document_chunks(pdf_path, "Documento PDF")

    assert len(chunks) == 1
    assert "Texto derivado de PDF" in chunks[0]["text"]


def test_extract_document_chunks_falls_back_to_antiword_for_doc_files(monkeypatch, tmp_path):
    doc_path = tmp_path / "source.doc"
    doc_path.write_bytes(b"fake doc")

    def _fake_partition_doc(filename):
        raise ModuleNotFoundError("No module named 'docx'")

    def _fake_partition_text(text=None, filename=None):
        assert "Texto extraído con antiword" in text
        return ["Texto antiword"]

    monkeypatch.setitem(
        sys.modules,
        "unstructured.partition.doc",
        SimpleNamespace(partition_doc=_fake_partition_doc),
    )
    monkeypatch.setitem(
        sys.modules,
        "unstructured.partition.text",
        SimpleNamespace(partition_text=_fake_partition_text),
    )
    monkeypatch.setattr(
        scraper_document_workflow.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout="Texto extraído con antiword",
            stderr="",
        ),
    )

    chunks = scraper_document_workflow._extract_document_chunks(doc_path, "Documento DOC")

    assert len(chunks) == 1
    assert "Texto antiword" in chunks[0]["text"]


@pytest.mark.asyncio
async def test_extract_cas_documents_uses_year_range_and_download_urls(monkeypatch, tmp_path):
    calls = []

    class _FakeCASParser:
        async def search(self, sport=None, year=None):
            calls.append((sport, year))
            fake_case = SimpleNamespace(
                numero_caso=SimpleNamespace(valor=f"CAS {year}/A/1234"),
                titulo=f"Case {year}",
                fecha=SimpleNamespace(valor=date(year, 1, 1)),
                categoria_deporte=SimpleNamespace(value="football"),
                tipo_procedimiento=SimpleNamespace(value="appeal"),
                partes="Club A v. Club B",
                resumen="Doping dispute",
                url=SimpleNamespace(
                    valor=f"https://jurisprudence.tas-cas.org/Shared%20Documents/CAS-{year}-A-1234.pdf"
                ),
            )
            return [fake_case], False

    monkeypatch.setitem(
        sys.modules,
        "src.infrastructure.adapters.cas_llm_parser",
        SimpleNamespace(CASLLMParser=lambda: _FakeCASParser()),
    )

    result = await crawl4ai_activities.extract_cas_documents(
        max_results=10,
        sport=None,
        year_from=2023,
        year_to=2024,
        matter="doping",
        output_directory=str(tmp_path / "cas_data"),
    )

    assert result["success"] is True
    assert [doc["external_id"] for doc in result["documents"]] == [
        "CAS 2023/A/1234",
        "CAS 2024/A/1234",
    ]
    assert result["documents"][0]["pdf_url"].endswith("CAS-2023-A-1234.pdf")
    assert calls == [(None, 2023), (None, 2024)]
