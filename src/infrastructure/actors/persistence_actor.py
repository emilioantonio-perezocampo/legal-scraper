"""
SCJN Persistence Actor

Persists documents and embeddings for the SCJN scraper.
Implements ISCJNDocumentRepository and IEmbeddingRepository interfaces.
"""
import json
import asyncio
from pathlib import Path
from typing import Dict, Optional, List
from datetime import date

from .base import BaseActor
from .messages import (
    GuardarDocumento,
    GuardarEmbedding,
    DocumentoGuardado,
)
from src.domain.scjn_entities import (
    SCJNDocument,
    SCJNArticle,
    SCJNReform,
    DocumentEmbedding,
)
from src.domain.scjn_value_objects import (
    DocumentCategory,
    DocumentScope,
    DocumentStatus,
    SubjectMatter,
)


class SCJNPersistenceActor(BaseActor):
    """
    Actor that persists documents and embeddings.

    Handles:
    - GuardarDocumento: Save document to storage
    - GuardarEmbedding: Save embedding to storage
    - ("LOAD_DOCUMENT", id): Load document by ID
    - ("FIND_BY_Q_PARAM", q_param): Find document by q_param
    - ("EXISTS", q_param): Check if document exists
    - ("LIST_DOCUMENTS",): List all document IDs
    - ("EMBEDDING_COUNT",): Get total embedding count
    """

    def __init__(self, storage_dir: str = "storage"):
        """
        Initialize the persistence actor.

        Args:
            storage_dir: Directory to store documents and embeddings
        """
        super().__init__()
        self._storage_dir = Path(storage_dir)
        self._documents_dir = self._storage_dir / "documents"
        self._embeddings_dir = self._storage_dir / "embeddings"

        # In-memory caches
        self._documents: Dict[str, SCJNDocument] = {}
        self._q_param_index: Dict[str, str] = {}  # q_param -> doc_id
        self._embeddings: List[DocumentEmbedding] = []
        self._lock = asyncio.Lock()

    async def start(self):
        """Start the actor and create directories."""
        await super().start()
        self._documents_dir.mkdir(parents=True, exist_ok=True)
        self._embeddings_dir.mkdir(parents=True, exist_ok=True)
        await self._load_existing_documents()

    async def handle_message(self, message):
        """Handle incoming messages."""
        if isinstance(message, GuardarDocumento):
            return await self._save_document(message.document, message.correlation_id)

        elif isinstance(message, GuardarEmbedding):
            return await self._save_embedding(message.embedding)

        elif isinstance(message, tuple) and len(message) >= 1:
            cmd = message[0]

            if cmd == "LOAD_DOCUMENT" and len(message) >= 2:
                return self._documents.get(message[1])

            elif cmd == "FIND_BY_Q_PARAM" and len(message) >= 2:
                doc_id = self._q_param_index.get(message[1])
                return self._documents.get(doc_id) if doc_id else None

            elif cmd == "EXISTS" and len(message) >= 2:
                return message[1] in self._q_param_index

            elif cmd == "LIST_DOCUMENTS":
                return tuple(self._documents.keys())

            elif cmd == "EMBEDDING_COUNT":
                return len(self._embeddings)

        return None

    async def _load_existing_documents(self) -> None:
        """Load all existing document files from directory."""
        for doc_file in self._documents_dir.glob("*.json"):
            try:
                doc = await self._load_document_file(doc_file)
                if doc:
                    self._documents[doc.id] = doc
                    self._q_param_index[doc.q_param] = doc.id
            except Exception:
                continue

    async def _load_document_file(self, file_path: Path) -> Optional[SCJNDocument]:
        """Load a single document file."""
        try:
            loop = asyncio.get_event_loop()
            content = await loop.run_in_executor(None, file_path.read_text)
            data = json.loads(content)
            return self._deserialize_document(data)
        except (json.JSONDecodeError, KeyError, ValueError):
            return None

    def _deserialize_document(self, data: dict) -> SCJNDocument:
        """Deserialize document from JSON data."""
        # Parse articles
        articles = tuple(
            SCJNArticle(
                number=a.get("number", ""),
                title=a.get("title", ""),
                content=a.get("content", ""),
                reform_dates=tuple(a.get("reform_dates", [])),
            )
            for a in data.get("articles", [])
        )

        # Parse reforms
        reforms = tuple(
            SCJNReform(
                id=r.get("id", ""),
                q_param=r.get("q_param", ""),
                publication_date=date.fromisoformat(r["publication_date"])
                if r.get("publication_date")
                else None,
                publication_number=r.get("publication_number", ""),
                gazette_section=r.get("gazette_section", ""),
                text_content=r.get("text_content"),
                pdf_path=r.get("pdf_path"),
            )
            for r in data.get("reforms", [])
        )

        # Parse subject matters
        subject_matters = tuple(
            SubjectMatter(s) for s in data.get("subject_matters", [])
        )

        return SCJNDocument(
            id=data["id"],
            q_param=data.get("q_param", ""),
            title=data.get("title", ""),
            short_title=data.get("short_title", ""),
            category=DocumentCategory(data.get("category", "LEY")),
            scope=DocumentScope(data.get("scope", "FEDERAL")),
            status=DocumentStatus(data.get("status", "VIGENTE")),
            publication_date=date.fromisoformat(data["publication_date"])
            if data.get("publication_date")
            else None,
            expedition_date=date.fromisoformat(data["expedition_date"])
            if data.get("expedition_date")
            else None,
            state=data.get("state"),
            subject_matters=subject_matters,
            articles=articles,
            reforms=reforms,
            source_url=data.get("source_url", ""),
        )

    async def _save_document(
        self, document: SCJNDocument, correlation_id: str
    ) -> DocumentoGuardado:
        """Save document to file and memory."""
        async with self._lock:
            # Store in memory
            self._documents[document.id] = document
            self._q_param_index[document.q_param] = document.id

            # Serialize to file
            data = self._serialize_document(document)
            file_path = self._documents_dir / f"{document.id}.json"

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, lambda: file_path.write_text(json.dumps(data, indent=2))
            )

            return DocumentoGuardado(
                correlation_id=correlation_id,
                document_id=document.id,
            )

    def _serialize_document(self, document: SCJNDocument) -> dict:
        """Serialize document to JSON-compatible dict."""
        return {
            "id": document.id,
            "q_param": document.q_param,
            "title": document.title,
            "short_title": document.short_title,
            "category": document.category.value,
            "scope": document.scope.value,
            "status": document.status.value,
            "publication_date": document.publication_date.isoformat()
            if document.publication_date
            else None,
            "expedition_date": document.expedition_date.isoformat()
            if document.expedition_date
            else None,
            "state": document.state,
            "subject_matters": [s.value for s in document.subject_matters],
            "articles": [
                {
                    "number": a.number,
                    "title": a.title,
                    "content": a.content,
                    "reform_dates": list(a.reform_dates),
                }
                for a in document.articles
            ],
            "reforms": [
                {
                    "id": r.id,
                    "q_param": r.q_param,
                    "publication_date": r.publication_date.isoformat()
                    if r.publication_date
                    else None,
                    "publication_number": r.publication_number,
                    "gazette_section": r.gazette_section,
                    "text_content": r.text_content,
                    "pdf_path": r.pdf_path,
                }
                for r in document.reforms
            ],
            "source_url": document.source_url,
        }

    async def _save_embedding(self, embedding: DocumentEmbedding) -> dict:
        """Save embedding to memory."""
        async with self._lock:
            self._embeddings.append(embedding)
            return {"chunk_id": embedding.chunk_id, "saved": True}
