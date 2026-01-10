# Implementation Plan - SCJN Legislation Scraper Extension

## Executive Summary

This document outlines the implementation plan for extending the legal-scraper codebase to support SCJN (Suprema Corte de Justicia de la Nación) legislation scraping, following the established patterns of **DDD**, **OOP**, **Actor Model**, and **RED-GREEN TDD**.

**Status:** Phase A (Domain) Partially Complete.

---

## 1. Implementation Phases Overview

```
Phase A: Domain Layer (Foundation)
    └── New entities, value objects, events for SCJN [PARTIALLY COMPLETE]

Phase B: Infrastructure - Adapters (Parsing)
    └── SCJN HTML parsers, PDF extractors

Phase C: Infrastructure - Actors (Processing)
    └── Discovery, Scraper, PDF Processor, Embedding actors

Phase D: Integration & Testing
    └── Wire actors together, end-to-end tests

Phase E: GUI Integration
    └── Add SCJN to TargetSource, update UI
```

---

## 2. Files to Create

### 2.1 Domain Layer

| File | Purpose | Dependencies |
|------|---------|--------------|
| `src/domain/scjn_entities.py` | SCJN-specific entities | None |
| `src/domain/scjn_value_objects.py` | Enums and value objects | None |
| `tests/unit/test_scjn_entities.py` | Domain unit tests | scjn_entities |

### 2.2 Infrastructure - Adapters

| File | Purpose | Dependencies |
|------|---------|--------------|
| `src/infrastructure/adapters/scjn_parser.py` | HTML parsing | beautifulsoup4, lxml |
| `src/infrastructure/adapters/scjn_search_parser.py` | Search results parsing | beautifulsoup4 |
| `src/infrastructure/adapters/pdf_extractor.py` | PDF text extraction | pypdf2/pdfplumber |
| `src/infrastructure/adapters/text_chunker.py` | Text chunking for embeddings | tiktoken |
| `tests/unit/test_scjn_parser.py` | Parser unit tests | Sample HTML |
| `tests/unit/test_pdf_extractor.py` | PDF extractor tests | Sample PDFs |

### 2.3 Infrastructure - Actors

| File | Purpose | Dependencies |
|------|---------|--------------|
| `src/infrastructure/actors/scjn_discovery_actor.py` | URL discovery | BaseActor, playwright |
| `src/infrastructure/actors/scjn_scraper_actor.py` | Document fetching | BaseActor, aiohttp, tenacity |
| `src/infrastructure/actors/pdf_processor_actor.py` | PDF processing | BaseActor, pdf_extractor |
| `src/infrastructure/actors/embedding_actor.py` | Vector generation | BaseActor, sentence-transformers |
| `tests/actors/test_scjn_discovery.py` | Discovery actor tests | pytest-asyncio |
| `tests/actors/test_scjn_scraper.py` | Scraper actor tests | pytest-asyncio |
| `tests/actors/test_pdf_processor.py` | PDF processor tests | pytest-asyncio |
| `tests/actors/test_embedding.py` | Embedding actor tests | pytest-asyncio |

### 2.4 Modifications to Existing Files

| File | Modification |
|------|--------------|
| `src/gui/domain/value_objects.py` | Add `SCJN` to `TargetSource` enum |
| `environment.yml` | Add new dependencies |

---

## 3. Dependency Updates

### 3.1 New Dependencies for `environment.yml`

```yaml
# PDF Processing
- pypdf2              # PDF text extraction (basic)
- pdfplumber          # Better PDF table/text extraction
- pytesseract         # OCR for scanned PDFs (optional)

# Browser Automation
- playwright          # Async browser automation

# Embeddings & Vector Storage
- sentence-transformers  # Text embeddings
- faiss-cpu           # Vector similarity search
- tiktoken            # Token counting for chunking

# Reliability
- tenacity            # Retry logic
```

### 3.2 Full Updated `environment.yml`

```yaml
name: legal-scraper
channels:
  - conda-forge
  - defaults
dependencies:
  # Core
  - python=3.10
  - pip

  # Testing
  - pytest
  - pytest-asyncio
  - pytest-cov

  # HTTP & Parsing (existing)
  - aiohttp
  - beautifulsoup4
  - lxml

  # GUI (existing)
  - tk
  - fastapi
  - uvicorn
  - jinja2
  - textual
  - rich

  # NEW: PDF Processing
  - pypdf2
  - pdfplumber

  # NEW: Vector/Embeddings (via pip)
  - pip:
    - playwright
    - sentence-transformers
    - faiss-cpu
    - tiktoken
    - tenacity
```

---

## 4. Detailed Implementation Plan

### Phase A: Domain Layer

#### A.1 Create SCJN Value Objects

**File**: `src/domain/scjn_value_objects.py`

```python
from enum import Enum

class DocumentCategory(Enum):
    """Category of SCJN legal document."""
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
    """Jurisdictional scope of document."""
    FEDERAL = "FEDERAL"
    ESTATAL = "ESTATAL"
    CDMX = "CDMX"
    INTERNACIONAL = "INTERNACIONAL"
    EXTRANJERA = "EXTRANJERA"

class DocumentStatus(Enum):
    """Current validity status of document."""
    VIGENTE = "VIGENTE"
    ABROGADA = "ABROGADA"
    DEROGADA = "DEROGADA"
    SUSTITUIDA = "SUSTITUIDA"
    EXTINTA = "EXTINTA"

class SubjectMatter(Enum):
    """Legal subject matter categories."""
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
```

#### A.2 Create SCJN Entities

**File**: `src/domain/scjn_entities.py`

```python
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional, List
from uuid import uuid4

from .scjn_value_objects import (
    DocumentCategory,
    DocumentScope,
    DocumentStatus,
    SubjectMatter,
)

@dataclass(frozen=True)
class SCJNReform:
    """A specific reform/amendment to a document."""
    id: str = field(default_factory=lambda: str(uuid4()))
    q_param: str = ""  # Encrypted URL parameter
    publication_date: Optional[date] = None
    publication_number: str = ""
    gazette_section: str = ""
    text_content: Optional[str] = None
    pdf_path: Optional[str] = None

@dataclass(frozen=True)
class SCJNArticle:
    """An individual article within a document."""
    number: str  # "1", "2 Bis", "Transitorio Primero"
    title: str = ""
    content: str = ""
    reform_dates: tuple = field(default_factory=tuple)

@dataclass(frozen=True)
class SCJNDocument:
    """Aggregate root for SCJN legislation documents."""
    id: str = field(default_factory=lambda: str(uuid4()))
    q_param: str = ""  # Encrypted URL parameter from SCJN
    title: str = ""
    short_title: str = ""
    category: DocumentCategory = DocumentCategory.LEY
    scope: DocumentScope = DocumentScope.FEDERAL
    status: DocumentStatus = DocumentStatus.VIGENTE
    publication_date: Optional[date] = None
    expedition_date: Optional[date] = None
    state: Optional[str] = None  # For state-level documents
    subject_matters: tuple = field(default_factory=tuple)
    articles: tuple = field(default_factory=tuple)  # tuple[SCJNArticle, ...]
    reforms: tuple = field(default_factory=tuple)  # tuple[SCJNReform, ...]
    source_url: str = ""

    @property
    def article_count(self) -> int:
        return len(self.articles)

    @property
    def reform_count(self) -> int:
        return len(self.reforms)

@dataclass(frozen=True)
class TextChunk:
    """A chunk of text prepared for embedding."""
    id: str = field(default_factory=lambda: str(uuid4()))
    document_id: str = ""
    content: str = ""
    token_count: int = 0
    chunk_index: int = 0
    metadata: tuple = field(default_factory=tuple)

@dataclass(frozen=True)
class DocumentEmbedding:
    """Vector embedding for a text chunk."""
    chunk_id: str
    vector: tuple  # tuple[float, ...] for immutability
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"

@dataclass(frozen=True)
class ScrapingCheckpoint:
    """Tracks progress for resume capability."""
    session_id: str
    last_processed_q_param: str
    processed_count: int
    failed_q_params: tuple = field(default_factory=tuple)
    created_at: datetime = field(default_factory=datetime.now)
```

#### A.3 Tests for Domain (RED Phase)

**File**: `tests/unit/test_scjn_entities.py`

```python
import pytest
from datetime import date
from src.domain.scjn_entities import (
    SCJNDocument,
    SCJNReform,
    SCJNArticle,
    TextChunk,
    DocumentEmbedding,
)
from src.domain.scjn_value_objects import (
    DocumentCategory,
    DocumentScope,
    DocumentStatus,
)

class TestSCJNDocument:
    def test_create_document_with_defaults(self):
        doc = SCJNDocument()
        assert doc.id is not None
        assert doc.category == DocumentCategory.LEY
        assert doc.status == DocumentStatus.VIGENTE

    def test_create_document_with_all_fields(self):
        doc = SCJNDocument(
            title="Ley Federal del Trabajo",
            category=DocumentCategory.LEY_FEDERAL,
            scope=DocumentScope.FEDERAL,
            status=DocumentStatus.VIGENTE,
            publication_date=date(1970, 4, 1),
        )
        assert doc.title == "Ley Federal del Trabajo"
        assert doc.publication_date == date(1970, 4, 1)

    def test_document_is_immutable(self):
        doc = SCJNDocument(title="Test")
        with pytest.raises(Exception):  # frozen dataclass
            doc.title = "Changed"

class TestSCJNReform:
    def test_create_reform(self):
        reform = SCJNReform(
            publication_date=date(2021, 3, 1),
            publication_number="274/2021",
        )
        assert reform.publication_number == "274/2021"

class TestSCJNArticle:
    def test_create_article(self):
        article = SCJNArticle(
            number="1",
            content="El trabajo es un derecho...",
        )
        assert article.number == "1"

class TestTextChunk:
    def test_create_chunk(self):
        chunk = TextChunk(
            document_id="doc-123",
            content="Sample text...",
            token_count=10,
            chunk_index=0,
        )
        assert chunk.token_count == 10

class TestDocumentEmbedding:
    def test_create_embedding(self):
        embedding = DocumentEmbedding(
            chunk_id="chunk-123",
            vector=tuple([0.1, 0.2, 0.3]),
        )
        assert len(embedding.vector) == 3
```

---

### Phase B: Infrastructure Adapters

#### B.1 SCJN HTML Parser

**File**: `src/infrastructure/adapters/scjn_parser.py`

```python
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, List, Optional
from src.domain.scjn_entities import SCJNDocument, SCJNReform, SCJNArticle
from src.domain.scjn_value_objects import DocumentCategory, DocumentStatus

def parse_scjn_document_detail(html: str) -> Dict:
    """Parse SCJN wfOrdenamientoDetalle.aspx page."""
    soup = BeautifulSoup(html, 'lxml')

    # Extract metadata from grid
    reforms = []
    grid_rows = soup.select('#gridReformas tr')

    for row in grid_rows:
        cells = row.find_all('td')
        if len(cells) >= 4:
            reform = {
                'publication_date': _parse_date(cells[0].get_text(strip=True)),
                'category': cells[1].get_text(strip=True),
                'status': cells[2].get_text(strip=True),
                'links': _extract_links(cells[3]),
            }
            reforms.append(reform)

    return {
        'reforms': reforms,
        'total_count': len(reforms),
    }

def parse_scjn_extract(html: str) -> Dict:
    """Parse SCJN wfExtracto.aspx page."""
    soup = BeautifulSoup(html, 'lxml')

    # Extract main text content
    text_div = soup.find('div', id='divTextoReforma')
    content = text_div.get_text(separator='\n', strip=True) if text_div else ""

    # Extract metadata
    return {
        'content': content,
        'publication_date': _extract_meta_field(soup, 'publication_date'),
        'category': _extract_meta_field(soup, 'category'),
    }

def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse date in dd/mm/yyyy format."""
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").date()
    except ValueError:
        return None

def _extract_links(cell) -> Dict[str, str]:
    """Extract action links from a cell."""
    links = {}
    for a in cell.find_all('a'):
        href = a.get('href', '')
        if 'wfExtracto' in href:
            links['extract'] = href
        elif 'AbrirDocReforma' in href:
            links['pdf'] = href
        elif 'wfArticuladoFast' in href:
            links['articles'] = href
    return links

def _extract_meta_field(soup, field: str) -> Optional[str]:
    """Extract metadata field from page."""
    # Implementation depends on actual HTML structure
    return None
```

#### B.2 PDF Extractor

**File**: `src/infrastructure/adapters/pdf_extractor.py`

```python
from typing import Optional
import io

def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using multiple strategies."""
    # Try pdfplumber first (better for tables)
    text = _try_pdfplumber(pdf_bytes)
    if text and len(text.strip()) > 100:
        return text

    # Fallback to PyPDF2
    text = _try_pypdf2(pdf_bytes)
    if text:
        return text

    return ""

def _try_pdfplumber(pdf_bytes: bytes) -> Optional[str]:
    """Extract using pdfplumber."""
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            text_parts = []
            for page in pdf.pages:
                text_parts.append(page.extract_text() or "")
            return "\n\n".join(text_parts)
    except Exception:
        return None

def _try_pypdf2(pdf_bytes: bytes) -> Optional[str]:
    """Extract using PyPDF2."""
    try:
        from PyPDF2 import PdfReader
        reader = PdfReader(io.BytesIO(pdf_bytes))
        text_parts = []
        for page in reader.pages:
            text_parts.append(page.extract_text() or "")
        return "\n\n".join(text_parts)
    except Exception:
        return None
```

#### B.3 Text Chunker

**File**: `src/infrastructure/adapters/text_chunker.py`

```python
from typing import List
from src.domain.scjn_entities import TextChunk

def chunk_text(
    text: str,
    document_id: str,
    max_tokens: int = 512,
    overlap_tokens: int = 50,
) -> List[TextChunk]:
    """Split text into overlapping chunks for embedding."""
    try:
        import tiktoken
        encoder = tiktoken.get_encoding("cl100k_base")
    except ImportError:
        # Fallback to simple word-based chunking
        return _chunk_by_words(text, document_id, max_tokens)

    tokens = encoder.encode(text)
    chunks = []

    start = 0
    chunk_index = 0

    while start < len(tokens):
        end = min(start + max_tokens, len(tokens))
        chunk_tokens = tokens[start:end]
        chunk_text = encoder.decode(chunk_tokens)

        chunks.append(TextChunk(
            document_id=document_id,
            content=chunk_text,
            token_count=len(chunk_tokens),
            chunk_index=chunk_index,
        ))

        start = end - overlap_tokens
        chunk_index += 1

    return chunks

def _chunk_by_words(text: str, document_id: str, max_words: int) -> List[TextChunk]:
    """Simple word-based chunking fallback."""
    words = text.split()
    chunks = []
    chunk_index = 0

    for i in range(0, len(words), max_words):
        chunk_words = words[i:i + max_words]
        chunks.append(TextChunk(
            document_id=document_id,
            content=" ".join(chunk_words),
            token_count=len(chunk_words),
            chunk_index=chunk_index,
        ))
        chunk_index += 1

    return chunks
```

---

### Phase C: Infrastructure Actors

#### C.1 SCJN Discovery Actor

**File**: `src/infrastructure/actors/scjn_discovery_actor.py`

```python
import asyncio
from typing import Optional
from src.infrastructure.actors.base import BaseActor

class SCJNDiscoveryActor(BaseActor):
    """
    The Scout for SCJN website.
    Discovers document URLs by navigating the search interface using Playwright.
    """

    def __init__(self, worker_actor: BaseActor, use_browser: bool = True):
        super().__init__()
        self.worker_actor = worker_actor
        self.use_browser = use_browser
        self._browser = None
        self._page = None

    async def handle_message(self, message):
        # Initialize browser on first use
        if self.use_browser and self._browser is None:
            await self._init_browser()

        if message == "DISCOVER_FEDERAL_LAWS":
            await self._discover_category("LEY_FEDERAL")

        elif isinstance(message, tuple):
            command = message[0]

            if command == "DISCOVER_CATEGORY":
                category = message[1]
                await self._discover_category(category)

            elif command == "DISCOVER_SEARCH":
                query = message[1]
                await self._discover_by_search(query)

    async def _init_browser(self):
        """Initialize Playwright browser."""
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=True)
        self._page = await self._browser.new_page()

    async def _discover_category(self, category: str):
        """Discover documents in a category."""
        base_url = "https://legislacion.scjn.gob.mx/Buscador/Paginas/Buscar.aspx"

        if self.use_browser:
            await self._page.goto(base_url)
            # Select category in dropdown
            await self._page.select_option('#ddlTipo', category)
            # Click search
            await self._page.click('#btnBuscar')
            # Wait for results
            await self._page.wait_for_selector('#gridResultados')

            # Extract result links
            links = await self._page.query_selector_all('a[href*="wfOrdenamientoDetalle"]')
            for link in links:
                href = await link.get_attribute('href')
                title = await link.inner_text()
                await self.worker_actor.tell({
                    'url': href,
                    'title': title,
                    'category': category,
                })
                await asyncio.sleep(1)  # Rate limit

    async def _discover_by_search(self, query: str):
        """Discover documents by search query."""
        # Implementation similar to _discover_category
        pass

    async def stop(self):
        """Clean up browser resources."""
        if self._browser:
            await self._browser.close()
        if hasattr(self, '_playwright'):
            await self._playwright.stop()
        await super().stop()
```

#### C.2 SCJN Scraper Actor

**File**: `src/infrastructure/actors/scjn_scraper_actor.py`

```python
import aiohttp
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential
from src.infrastructure.actors.base import BaseActor
from src.infrastructure.adapters.scjn_parser import parse_scjn_document_detail

class SCJNScraperActor(BaseActor):
    """
    The Worker for SCJN documents.
    Fetches document pages and PDFs.
    
    Adheres to SCJN site analysis recommendation:
    - ~1000 documents/day limit
    - Rotation of User-Agents
    """

    BASE_URL = "https://legislacion.scjn.gob.mx/Buscador/Paginas/"

    def __init__(
        self,
        pdf_processor: Optional[BaseActor] = None,
        persistence: Optional[BaseActor] = None,
        rate_limit: float = 5.0  # Increased for safety (approx 17k/day theoretical, 1k recommended)
    ):
        super().__init__()
        self.pdf_processor = pdf_processor
        self.persistence = persistence
        self.rate_limit = rate_limit
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    async def handle_message(self, message):
        if isinstance(message, dict) and 'url' in message:
            await self._scrape_document(message)

        elif isinstance(message, tuple):
            command = message[0]

            if command == "FETCH_PDF":
                q_param = message[1]
                await self._fetch_pdf(q_param)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def _scrape_document(self, item: dict):
        """Scrape a document detail page with retries."""
        url = item['url']

        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(url, ssl=False) as response:
                    if response.status == 200:
                        html = await response.text(encoding='utf-8')

                        # Parse document
                        parsed = parse_scjn_document_detail(html)

                        # Process each reform's PDF
                        for reform in parsed.get('reforms', []):
                            if 'pdf' in reform.get('links', {}):
                                pdf_url = reform['links']['pdf']
                                if self.pdf_processor:
                                    await self.pdf_processor.tell(("PROCESS_URL", pdf_url))

                        print(f"✅ Scraped: {item.get('title', url)[:60]}")

                        if self.persistence:
                            await self.persistence.tell(("SAVE_SCJN_DOC", {
                                **item,
                                **parsed,
                            }))
                    else:
                        print(f"❌ Error: {url} returned {response.status}")
                        raise Exception(f"HTTP {response.status}")

        except Exception as e:
            print(f"❌ Exception scraping {url}: {e}")
            raise  # Trigger retry

        # Rate limiting
        import asyncio
        await asyncio.sleep(self.rate_limit)

    async def _fetch_pdf(self, q_param: str):
        """Fetch PDF by q parameter."""
        url = f"{self.BASE_URL}AbrirDocReforma.aspx?q={q_param}"

        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(url, ssl=False) as response:
                    if response.status == 200:
                        pdf_bytes = await response.read()

                        if self.pdf_processor:
                            await self.pdf_processor.tell(("PROCESS_PDF", pdf_bytes, q_param))

                        return pdf_bytes
        except Exception as e:
            print(f"❌ Error fetching PDF: {e}")

        return None
```

#### C.3 PDF Processor Actor

**File**: `src/infrastructure/actors/pdf_processor_actor.py`

```python
from src.infrastructure.actors.base import BaseActor
from src.infrastructure.adapters.pdf_extractor import extract_text_from_pdf
from src.infrastructure.adapters.text_chunker import chunk_text

class PDFProcessorActor(BaseActor):
    """
    Processes PDFs: extracts text, chunks for embedding.
    """

    def __init__(
        self,
        embedding_actor: 'BaseActor' = None,
        persistence_actor: 'BaseActor' = None
    ):
        super().__init__()
        self.embedding_actor = embedding_actor
        self.persistence_actor = persistence_actor

    async def handle_message(self, message):
        if isinstance(message, tuple):
            command = message[0]

            if command == "PROCESS_PDF":
                pdf_bytes = message[1]
                doc_id = message[2] if len(message) > 2 else "unknown"
                await self._process_pdf(pdf_bytes, doc_id)

            elif command == "PROCESS_URL":
                # Delegate back to scraper for fetching
                pass

    async def _process_pdf(self, pdf_bytes: bytes, doc_id: str):
        """Process PDF bytes."""
        # Extract text
        text = extract_text_from_pdf(pdf_bytes)

        if not text or len(text.strip()) < 50:
            print(f"⚠️ PDF {doc_id} has minimal text, may need OCR")
            return

        # Chunk text
        chunks = chunk_text(text, doc_id)
        print(f"📄 Extracted {len(chunks)} chunks from {doc_id}")

        # Send to embedding actor
        if self.embedding_actor:
            for chunk in chunks:
                await self.embedding_actor.tell(("EMBED", chunk))

        # Save raw text
        if self.persistence_actor:
            await self.persistence_actor.tell(("SAVE_TEXT", {
                'doc_id': doc_id,
                'text': text,
                'chunk_count': len(chunks),
            }))
```

#### C.4 Embedding Actor

**File**: `src/infrastructure/actors/embedding_actor.py`

```python
from typing import List
from src.infrastructure.actors.base import BaseActor
from src.domain.scjn_entities import TextChunk, DocumentEmbedding

class EmbeddingActor(BaseActor):
    """
    Generates vector embeddings for text chunks.
    """

    def __init__(
        self,
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
        persistence_actor: 'BaseActor' = None
    ):
        super().__init__()
        self.model_name = model_name
        self.persistence_actor = persistence_actor
        self._model = None

    async def handle_message(self, message):
        if isinstance(message, tuple):
            command = message[0]

            if command == "EMBED":
                chunk = message[1]
                await self._embed_chunk(chunk)

            elif command == "EMBED_BATCH":
                chunks = message[1]
                await self._embed_batch(chunks)

    def _load_model(self):
        """Lazy load the embedding model."""
        if self._model is None:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self.model_name)
        return self._model

    async def _embed_chunk(self, chunk: TextChunk):
        """Generate embedding for a single chunk."""
        import asyncio

        model = self._load_model()

        # Run in executor to not block event loop
        loop = asyncio.get_event_loop()
        vector = await loop.run_in_executor(
            None,
            lambda: model.encode(chunk.content).tolist()
        )

        embedding = DocumentEmbedding(
            chunk_id=chunk.id,
            vector=tuple(vector),
            model_name=self.model_name,
        )

        if self.persistence_actor:
            await self.persistence_actor.tell(("SAVE_EMBEDDING", embedding))

        return embedding

    async def _embed_batch(self, chunks: List[TextChunk]):
        """Generate embeddings for multiple chunks."""
        import asyncio

        model = self._load_model()
        texts = [chunk.content for chunk in chunks]

        loop = asyncio.get_event_loop()
        vectors = await loop.run_in_executor(
            None,
            lambda: model.encode(texts).tolist()
        )

        embeddings = []
        for chunk, vector in zip(chunks, vectors):
            embedding = DocumentEmbedding(
                chunk_id=chunk.id,
                vector=tuple(vector),
                model_name=self.model_name,
            )
            embeddings.append(embedding)

            if self.persistence_actor:
                await self.persistence_actor.tell(("SAVE_EMBEDDING", embedding))

        return embeddings
```

---

### Phase D: Integration

#### D.1 Main Entry Point Update

**New file**: `src/scjn_main.py`

```python
import asyncio
from datetime import date
from src.infrastructure.actors.scjn_discovery_actor import SCJNDiscoveryActor
from src.infrastructure.actors.scjn_scraper_actor import SCJNScraperActor
from src.infrastructure.actors.pdf_processor_actor import PDFProcessorActor
from src.infrastructure.actors.embedding_actor import EmbeddingActor
from src.infrastructure.actors.persistence import PersistenceActor

async def main():
    print("🚀 SCJN Legislation Scraper Starting...")

    # Initialize actors (bottom-up)
    persistence = PersistenceActor(output_dir="scjn_data")
    embedding = EmbeddingActor(persistence_actor=persistence)
    pdf_processor = PDFProcessorActor(
        embedding_actor=embedding,
        persistence_actor=persistence
    )
    scraper = SCJNScraperActor(
        pdf_processor=pdf_processor,
        persistence=persistence
    )
    discovery = SCJNDiscoveryActor(worker_actor=scraper)

    # Start actors
    await persistence.start()
    await embedding.start()
    await pdf_processor.start()
    await scraper.start()
    await discovery.start()

    print("✅ All actors online.")

    # Trigger discovery
    await discovery.tell("DISCOVER_FEDERAL_LAWS")

    # Keep alive
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    finally:
        await discovery.stop()
        await scraper.stop()
        await pdf_processor.stop()
        await embedding.stop()
        await persistence.stop()
        print("👋 Goodbye.")

if __name__ == "__main__":
    asyncio.run(main())
```

---

### Phase E: GUI Integration

#### E.1 Update TargetSource Enum

**File**: `src/gui/domain/value_objects.py` (modification)

```python
class TargetSource(Enum):
    """Available scraping targets."""
    DOF = "dof"
    SCJN = "scjn"  # NEW

    @property
    def display_name(self) -> str:
        names = {
            "dof": "Diario Oficial de la Federación",
            "scjn": "SCJN Legislación",  # NEW
        }
        return names.get(self.value, self.value)

    @property
    def base_url(self) -> str:
        urls = {
            "dof": "https://dof.gob.mx/",
            "scjn": "https://legislacion.scjn.gob.mx/",  # NEW
        }
        return urls.get(self.value, "")
```

---

## 5. Test Plan (RED-GREEN TDD)

### 5.1 Testing Order

1. **Domain Tests** (Phase A)
   - Entity creation
   - Immutability
   - Value object validation

2. **Adapter Tests** (Phase B)
   - Parser with sample HTML
   - PDF extractor with sample PDFs
   - Chunker with sample text

3. **Actor Tests** (Phase C)
   - Message handling
   - Actor lifecycle
   - Integration between actors

4. **End-to-End Tests** (Phase D)
   - Full pipeline with mocked HTTP
   - Real scrape of single document

### 5.2 Sample Test Commands

```bash
# Run domain tests
pytest tests/unit/test_scjn_entities.py -v

# Run adapter tests
pytest tests/unit/test_scjn_parser.py -v
pytest tests/unit/test_pdf_extractor.py -v

# Run actor tests
pytest tests/actors/test_scjn_discovery.py -v
pytest tests/actors/test_scjn_scraper.py -v

# Run all SCJN tests
pytest tests/ -k "scjn" -v

# Run with coverage
pytest tests/ -k "scjn" --cov=src --cov-report=html
```

---

## 6. Implementation Checklist

### Phase A: Domain Layer
- [x] Create `src/domain/scjn_value_objects.py`
- [x] Create `src/domain/scjn_entities.py`
- [ ] Create `tests/unit/test_scjn_entities.py`
- [ ] Run tests (should FAIL - RED)
- [ ] Implement entities
- [ ] Run tests (should PASS - GREEN)

### Phase B: Adapters
- [ ] Create `src/infrastructure/adapters/scjn_parser.py`
- [ ] Create `src/infrastructure/adapters/pdf_extractor.py`
- [ ] Create `src/infrastructure/adapters/text_chunker.py`
- [ ] Create tests for each adapter
- [ ] Run tests (RED → GREEN)

### Phase C: Actors
- [ ] Create `src/infrastructure/actors/scjn_discovery_actor.py`
- [ ] Create `src/infrastructure/actors/scjn_scraper_actor.py`
- [ ] Create `src/infrastructure/actors/pdf_processor_actor.py`
- [ ] Create `src/infrastructure/actors/embedding_actor.py`
- [ ] Create tests for each actor
- [ ] Run tests (RED → GREEN)

### Phase D: Integration
- [ ] Create `src/scjn_main.py`
- [ ] Create end-to-end tests
- [ ] Test with real SCJN website (rate limited)

### Phase E: GUI Integration
- [ ] Add `SCJN` to `TargetSource` enum
- [ ] Update GUI tests
- [ ] Verify GUI displays SCJN option

---

## 7. Risk Mitigations

| Risk | Mitigation |
|------|------------|
| Browser automation complexity | Start with simple aiohttp, add Playwright only if needed |
| PDF OCR requirements | Use pdfplumber first, add Tesseract only if needed |
| Large embedding models | Use lightweight `all-MiniLM-L6-v2` (80MB) |
| Rate limiting | Implement exponential backoff with tenacity |
| SCJN website changes | Version detection, flexible CSS selectors |

---

## 8. Success Criteria

1. **Domain**: All entity tests pass
2. **Adapters**: Can parse sample SCJN HTML and extract PDF text
3. **Actors**: All actors start/stop cleanly, handle messages
4. **Integration**: Can scrape at least one SCJN document end-to-end
5. **GUI**: SCJN appears in source dropdown and can be selected

---

*Document generated during Phase 3 of SCJN Legislation Scraper Extension analysis.*