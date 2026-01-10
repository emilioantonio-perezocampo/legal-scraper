# Legal Scraper Architecture

## High-Level Overview
This project is an asynchronous, distributed-style web scraping system built on the **Actor Model**. It is designed to extract, normalize, and store legal documents from Mexican government portals (DOF and SCJN) with resilience, rate-limiting, and multiple user interfaces (CLI, TUI, Web).

## Tech Stack
*   **Language:** Python 3.10+
*   **Concurrency:** `asyncio` (Native coroutines)
*   **Networking:** `aiohttp` (Async HTTP client)
*   **Parsing:** `beautifulsoup4`, `lxml` (HTML/XML processing)
*   **PDF Processing:** `pypdf2`, `pdfplumber` (Text extraction)
*   **Data Models:** Python `dataclasses` (frozen/immutable preferred)
*   **Interfaces:**
    *   **Web:** `fastapi`, `uvicorn`, `jinja2`
    *   **Terminal (TUI):** `textual`, `rich`
    *   **Desktop:** `tkinter`
*   **Testing:** `pytest`, `pytest-asyncio`, `pytest-cov`

## Project Structure
```text
src/
├── domain/                  # PURE BUSINESS LOGIC (No I/O, No Frameworks)
│   ├── entities.py          # Core mutable entities (legacy)
│   ├── scjn_entities.py     # New immutable entities (SCJN)
│   └── value_objects.py     # Domain enums and simple types
├── infrastructure/          # IMPLEMENTATION DETAILS (I/O, Actors)
│   ├── actors/              # Actor Model Implementation
│   │   ├── base.py          # Abstract BaseActor (Queue-based)
│   │   ├── dof_*.py         # DOF-specific actors
│   │   └── scjn_*.py        # SCJN-specific actors
│   └── adapters/            # External System Connectors
│       ├── *_parser.py      # HTML/Text parsers
│       └── pdf_extractor.py # PDF processing logic
├── gui/                     # USER INTERFACE LAYER
│   ├── application/         # UI Logic / Use Cases
│   ├── presentation/        # View Models & Presenters
│   └── [web|tui|desktop]/   # Framework-specific implementations
└── main.py                  # CLI/Backend Entry Point
```

## Key Patterns

### 1. Actor Model
The system does not use threads or OS processes directly. Instead, it uses **Asyncio Actors**:
*   **Component:** `BaseActor` (Abstract Class)
*   **Communication:**
    *   `tell(msg)`: Fire-and-forget (Async, Non-blocking).
    *   `ask(msg)`: Request-Response (Returns `asyncio.Future`).
*   **State:** Each actor maintains its own private state (`self._state`). Shared state is avoided.
*   **Orchestration:** `SchedulerActor` triggers workflows; `DiscoveryActor` feeds `ScraperActor`; `PersistenceActor` saves results.

### 2. Domain-Driven Design (DDD)
*   **Entities:** Defined in `src/domain`. They encapsulate data and valid operations.
*   **Independence:** Domain code NEVER imports from `infrastructure` or `gui`.
*   **Immutability:** Newer modules (SCJN) strictly use `frozen=True` dataclasses and tuples to prevent side effects.

### 3. Pipeline Architecture
Data flows in a unidirectional stream:
1.  **Discovery:** Finds URLs (e.g., from Index page).
2.  **Scraping:** Fetches Raw HTML/PDF.
3.  **Processing:** Parses text, extracts metadata, chunks content.
4.  **Persistence:** Saves structured JSON/Vectors to disk.

## Business Rules
1.  **Rate Limiting:** SCJN scraping is strictly limited (recommended ~1000 docs/day) to prevent IP bans.
2.  **Resilience:** Network calls must employ retry logic (using `tenacity`) and user-agent rotation.
3.  **Data Sovereignty:** Scraped data is stored locally in `scraped_data/` or `data/` JSON files; it is the source of truth for the system.
