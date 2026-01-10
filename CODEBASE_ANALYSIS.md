# Codebase Analysis - Legal Scraper Repository

## Executive Summary

This document provides a comprehensive analysis of the existing legal-scraper codebase in preparation for adding the SCJN (Supreme Court of Justice of Mexico) Legislation Scraper extension. The analysis follows the project's established patterns: **DDD (Domain-Driven Design)**, **OOP**, and **Actor Model** architecture.

---

## 1. Repository Structure

```
legal-scrapper/
├── src/
│   ├── domain/
│   │   └── entities.py              # Core domain entities (FederalLaw, Article)
│   ├── infrastructure/
│   │   ├── actors/
│   │   │   ├── base.py              # BaseActor ABC (tell/ask patterns)
│   │   │   ├── scheduler.py         # SchedulerActor (orchestrator)
│   │   │   ├── dof_actor.py         # DofScraperActor (worker)
│   │   │   ├── dof_discovery_actor.py  # DofDiscoveryActor (scout)
│   │   │   └── persistence.py       # PersistenceActor (storage)
│   │   └── adapters/
│   │       ├── dof_parser.py        # HTML parser for DOF documents
│   │       └── dof_index_parser.py  # Index page parser
│   ├── gui/                         # GUI module (recently added)
│   │   ├── domain/                  # GUI domain entities, events, value objects
│   │   ├── infrastructure/actors/   # GUI-specific actors
│   │   ├── presentation/            # View models, presenters
│   │   ├── application/             # Use cases, services
│   │   ├── web/                     # FastAPI REST API
│   │   └── tui/                     # Textual terminal TUI
│   └── main.py                      # Entry point
├── tests/
│   ├── unit/                        # Unit tests for domain and parsers
│   ├── actors/                      # Actor integration tests
│   └── gui/                         # GUI tests (119 tests)
└── environment.yml                  # Conda dependencies
```

---

## 2. Existing Domain Model

### 2.1 Core Entities (`src/domain/entities.py`)

```python
@dataclass
class Article:
    """Value Object: Represents a single article within a law."""
    identifier: str  # e.g., "Art. 1", "Art. 42 Bis"
    content: str
    order: int  # Maintains sequence

@dataclass
class FederalLaw:
    """Aggregate Root: Represents a Federal Law."""
    title: str
    publication_date: date
    jurisdiction: str
    articles: List[Article] = field(default_factory=list)
```

### 2.2 Key Observations

| Aspect | Current Implementation | Notes for SCJN Extension |
|--------|------------------------|--------------------------|
| **Immutability** | Uses mutable dataclasses | GUI entities use `frozen=True`; consider for new entities |
| **Article Model** | Generic with identifier/content/order | Works for SCJN laws |
| **Jurisdiction** | String field | Could extend to enum for SCJN categories |
| **No PDF Support** | HTML-only parsing | SCJN requires PDF handling |
| **No Embeddings** | Plain text storage | SCJN extension needs vector embeddings |

---

## 3. Actor Model Architecture

### 3.1 Base Actor (`src/infrastructure/actors/base.py`)

```python
class BaseActor(ABC):
    """Abstract Base Actor with Queue-based mailbox."""

    def __init__(self):
        self._queue = asyncio.Queue()
        self._running = False
        self._task = None

    async def start(self):
        """Starts the actor's processing loop."""
        self._running = True
        self._task = asyncio.create_task(self._process_mailbox())

    async def stop(self):
        """Stops the actor cleanly via poison pill."""
        self._running = False
        await self._queue.put(None)
        if self._task:
            await self._task

    async def tell(self, message):
        """Fire and forget: Send message without waiting."""
        await self._queue.put(message)

    async def ask(self, message):
        """Request-Response: Send message and wait for reply."""
        future = asyncio.get_running_loop().create_future()
        await self._queue.put((message, future))
        return await future

    @abstractmethod
    async def handle_message(self, message):
        """Subclasses must implement message handling."""
        pass
```

### 3.2 Actor Hierarchy

```
                    ┌─────────────────┐
                    │ SchedulerActor  │  (Orchestrator)
                    └────────┬────────┘
                             │ REGISTER_WORKER / TRIGGER_NOW
                             ▼
                    ┌─────────────────┐
                    │DofDiscoveryActor│  (Scout)
                    └────────┬────────┘
                             │ tell({url, title})
                             ▼
                    ┌─────────────────┐
                    │ DofScraperActor │  (Worker)
                    └────────┬────────┘
                             │ tell(("SAVE_LAW", law))
                             ▼
                    ┌─────────────────┐
                    │PersistenceActor │  (Storage)
                    └─────────────────┘
```

### 3.3 Message Protocol Patterns

| Message Type | Format | Actor | Description |
|--------------|--------|-------|-------------|
| `DISCOVER_TODAY` | String | Discovery | Scan today's index |
| `DISCOVER_DATE` | `("DISCOVER_DATE", date)` | Discovery | Scan specific date |
| `DISCOVER_RANGE` | `("DISCOVER_RANGE", start, end)` | Discovery | Historical backfill |
| `{url, title}` | Dict | Scraper | Rich scrape request |
| `String URL` | String | Scraper | Legacy scrape request |
| `SAVE_LAW` | `("SAVE_LAW", FederalLaw)` | Persistence | Store law to disk |
| `REGISTER_WORKER` | `("REGISTER_WORKER", actor)` | Scheduler | Register actor |
| `TRIGGER_NOW` | String | Scheduler | Broadcast start |

---

## 4. Infrastructure Adapters

### 4.1 DOF Parser (`src/infrastructure/adapters/dof_parser.py`)

**Responsibilities:**
- Parse DOF HTML into `FederalLaw` entity
- Extract: title, publication date, articles
- Handle both structured laws and unstructured notices

**Key Strategies:**
1. **Date Extraction**: Look for `#lblFecha` span or "DOF: dd/mm/yyyy" text
2. **Title Extraction**: `h3.titulo` or first bold in centered paragraph
3. **Article Extraction**: `.Articulo` divs or fallback to `<p align="justify">`

### 4.2 DOF Index Parser (`src/infrastructure/adapters/dof_index_parser.py`)

**Responsibilities:**
- Parse daily index page HTML
- Return list of `{url, title}` dictionaries
- Deduplicate URLs

**Pattern:**
```python
def parse_dof_index(html_content: str) -> List[Dict[str, str]]:
    """Returns [{"url": "...", "title": "..."}, ...]"""
```

---

## 5. GUI Module Architecture

The GUI module (recently added) follows a **clean architecture** pattern:

### 5.1 Layers

```
┌─────────────────────────────────────────────────────────┐
│                    Presentation Layer                    │
│   ┌───────────┐  ┌───────────┐  ┌──────────────────┐   │
│   │  Desktop  │  │    Web    │  │       TUI        │   │
│   │ (Tkinter) │  │ (FastAPI) │  │    (Textual)     │   │
│   └───────────┘  └───────────┘  └──────────────────┘   │
├─────────────────────────────────────────────────────────┤
│                    Application Layer                     │
│   ┌─────────────────────┐  ┌─────────────────────────┐  │
│   │      Use Cases      │  │       Services          │  │
│   │ (Start/Pause/Cancel)│  │ (ScraperService)        │  │
│   └─────────────────────┘  └─────────────────────────┘  │
├─────────────────────────────────────────────────────────┤
│                   Infrastructure Layer                   │
│   ┌───────────────────────────────────────────────────┐ │
│   │                    GUI Actors                      │ │
│   │ GuiStateActor │ GuiControllerActor │ GuiBridgeActor│ │
│   └───────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────┤
│                      Domain Layer                        │
│   ┌────────────┐  ┌───────────────┐  ┌──────────────┐  │
│   │  Entities  │  │ Value Objects │  │    Events    │  │
│   │ ScraperJob │  │  TargetSource │  │ JobStarted   │  │
│   │ JobProgress│  │  ScraperMode  │  │ JobCompleted │  │
│   └────────────┘  └───────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### 5.2 GUI Domain Entities

| Entity | Description | Immutable |
|--------|-------------|-----------|
| `ScraperJob` | Aggregate root for job lifecycle | Yes (`frozen=True`) |
| `JobProgress` | Tracks items processed/success/failed | Yes |
| `ScraperConfiguration` | Job settings (source, mode, dates) | Yes |
| `LogEntry` | Captured log during execution | Yes |
| `DateRange` | Validated date range value object | Yes |

### 5.3 GUI Value Objects

| Value Object | Values | Purpose |
|--------------|--------|---------|
| `TargetSource` | `DOF` | Scraping source (extensible for SCJN) |
| `ScraperMode` | `TODAY`, `SINGLE_DATE`, `DATE_RANGE`, `HISTORICAL` | Job mode |
| `OutputFormat` | `JSON`, `CSV`, `XML` | Output file format |

---

## 6. Testing Patterns

### 6.1 Test Organization

```
tests/
├── unit/                    # Domain and adapter unit tests
│   ├── test_federal_law.py
│   ├── test_dof_parser.py
│   └── test_dof_index_parser.py
├── actors/                  # Actor integration tests
│   ├── test_base_actor.py
│   ├── test_scheduler.py
│   ├── test_persistence.py
│   └── test_dof_actor.py
└── gui/                     # GUI module tests (119 tests)
    ├── domain/
    ├── actors/
    ├── infrastructure/
    ├── application/
    ├── web/
    └── tui/
```

### 6.2 Test Patterns Used

1. **Async Tests**: `@pytest.mark.asyncio` decorator
2. **Actor Tests**: Start → Tell/Ask → Assert → Stop
3. **Parser Tests**: Sample HTML → Parse → Assert entity
4. **GUI Tests**: RED-GREEN TDD (failing tests first)

### 6.3 Sample Test Patterns

```python
# Actor test pattern
@pytest.mark.asyncio
async def test_actor_ask_pattern():
    actor = EchoActor()
    await actor.start()
    response = await actor.ask("ping")
    assert response == "pong"
    await actor.stop()

# Parser test pattern
def test_parse_dof_html_extracts_law():
    law = parse_dof_html(SAMPLE_HTML)
    assert isinstance(law, FederalLaw)
    assert law.title == "LEY FEDERAL DEL TRABAJO"
```

---

## 7. Dependencies

### Current (`environment.yml`)

| Category | Package | Version | Purpose |
|----------|---------|---------|---------|
| Core | python | 3.10 | Runtime |
| Testing | pytest | - | Test framework |
| Testing | pytest-asyncio | - | Async test support |
| Testing | pytest-cov | - | Coverage reports |
| HTTP | aiohttp | - | Async HTTP client |
| Parsing | beautifulsoup4 | - | HTML parser |
| Parsing | lxml | - | Fast parser engine |
| GUI Desktop | tk | - | Tkinter |
| GUI Web | fastapi | - | REST API framework |
| GUI Web | uvicorn | - | ASGI server |
| GUI Web | jinja2 | - | HTML templates |
| GUI TUI | textual | - | Terminal UI |
| GUI TUI | rich | - | Rich text formatting |

### Required for SCJN Extension

| Package | Purpose | Notes |
|---------|---------|-------|
| `pypdf2` or `pdfplumber` | PDF text extraction | For SCJN PDFs |
| `playwright` or `selenium` | Browser automation | Dynamic content |
| `sentence-transformers` | Text embeddings | For vector search |
| `faiss-cpu` or `chromadb` | Vector storage | Embedding DB |
| `tiktoken` | Token counting | For chunking |

---

## 8. Entry Points

### 8.1 Scraper Main (`src/main.py`)

```python
async def main():
    # Initialize actors
    persistence = PersistenceActor(output_dir="scraped_data")
    dof_scraper = DofScraperActor(output_actor=persistence)
    dof_discovery = DofDiscoveryActor(worker_actor=dof_scraper)
    scheduler = SchedulerActor()

    # Start actors
    await persistence.start()
    await dof_scraper.start()
    await dof_discovery.start()
    await scheduler.start()

    # Trigger historical scrape
    await dof_discovery.tell(("DISCOVER_RANGE", start_date, end_date))

    # Keep alive loop
    ...
```

### 8.2 GUI Launcher (`src/gui/launcher.py`)

```python
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", choices=["web", "tui", "desktop"])

    if args.mode == "web":
        run_web_gui(host=args.host, port=args.port)
    elif args.mode == "tui":
        run_tui_gui()
    elif args.mode == "desktop":
        run_desktop_gui()
```

---

## 9. Patterns to Follow for SCJN Extension

### 9.1 Domain Layer

- Create new entities in `src/domain/` (e.g., `scjn_entities.py`)
- Use `@dataclass(frozen=True)` for immutability
- Define clear aggregate roots

### 9.2 Actor Layer

- Extend `BaseActor` for new actors
- Follow message protocol patterns
- Use tuple messages for commands with parameters

### 9.3 Adapter Layer

- Create new parsers in `src/infrastructure/adapters/`
- Keep parsing logic separate from actors
- Return domain entities from parsers

### 9.4 Testing

- Follow RED-GREEN TDD
- Create tests BEFORE implementation
- Use `@pytest.mark.asyncio` for async tests
- Mirror directory structure in `tests/`

---

## 10. Identified Extension Points for SCJN

### 10.1 New Files to Create

| File | Layer | Purpose |
|------|-------|---------|
| `src/domain/scjn_entities.py` | Domain | SCJN-specific entities |
| `src/infrastructure/actors/scjn_discovery_actor.py` | Infrastructure | SCJN index scraper |
| `src/infrastructure/actors/scjn_scraper_actor.py` | Infrastructure | SCJN document scraper |
| `src/infrastructure/actors/pdf_processor_actor.py` | Infrastructure | PDF extraction |
| `src/infrastructure/actors/embedding_actor.py` | Infrastructure | Vector embeddings |
| `src/infrastructure/adapters/scjn_parser.py` | Infrastructure | SCJN HTML parser |
| `src/infrastructure/adapters/pdf_extractor.py` | Infrastructure | PDF text extraction |
| `tests/unit/test_scjn_entities.py` | Tests | Domain tests |
| `tests/unit/test_scjn_parser.py` | Tests | Parser tests |
| `tests/actors/test_scjn_actors.py` | Tests | Actor tests |

### 10.2 Modifications to Existing Files

| File | Modification |
|------|--------------|
| `src/gui/domain/value_objects.py` | Add `SCJN` to `TargetSource` enum |
| `environment.yml` | Add PDF and embedding dependencies |

---

## 11. Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| SCJN website structure changes | High | Robust parsing with fallbacks |
| IP blocking by SCJN | Medium | Rate limiting, user-agent rotation |
| PDF extraction quality | Medium | Multiple extraction strategies |
| Large file handling | Low | Streaming, chunking |
| Embedding model size | Low | Use lightweight models |

---

## 12. Recommendations

1. **Follow existing patterns**: The codebase has well-established DDD/Actor patterns. New code should match.

2. **Extend, don't modify**: Add `SCJN` to `TargetSource` enum rather than creating separate systems.

3. **RED-GREEN TDD**: Write failing tests first for all new functionality.

4. **PDF processing as Actor**: Create dedicated `PDFProcessorActor` to maintain actor model consistency.

5. **Embeddings as optional**: Keep embedding generation as a separate, optional actor for flexibility.

6. **GUI integration**: Update `TargetSource` enum and GUI will automatically support SCJN.

---

*Document generated during Phase 1 of SCJN Legislation Scraper Extension analysis.*
