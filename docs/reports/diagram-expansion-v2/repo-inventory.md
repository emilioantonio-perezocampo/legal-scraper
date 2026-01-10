# Repo Inventory

**Date:** 2026-01-05
**Scope:** `src/` directory and root configuration files.

## 1. System Entry Points
The system is composed of multiple specialized scrapers and a GUI/TUI interface, all sharing a common architectural core (Actor Model).

| Component | Entry Point File | CLI Command Pattern | Key Classes |
| :--- | :--- | :--- | :--- |
| **DOF Scraper** | `src/main.py` | `python src/main.py` | `SchedulerActor`, `DofScraperActor`, `PersistenceActor`, `DofDiscoveryActor` |
| **SCJN Scraper** | `src/scjn_main.py` | `discover`, `status`, `resume` | `SCJNCoordinatorActor`, `SCJNDiscoveryActor`, `SCJNScraperActor` |
| **BJV Scraper** | `src/bjv_main.py` | `discover`, `scrape`, `status`, `resume` | `BJVCoordinatorActor`, `BJVSessionManager` |
| **CAS Scraper** | `src/cas_main.py` | `discover`, `config` | Delegates to `src.infrastructure.cli.cas_cli` |
| **GUI/TUI** | `src/gui/launcher.py` | (Inferred) | `src.gui.tui.app`, `src.gui.web.api` |

## 2. Architecture & Concurrency
*   **Pattern:** **Actor Model** using `asyncio`.
*   **Base Class:** `src.infrastructure.actors.base.BaseActor` implements the mailbox pattern (`tell`/`ask`) with an internal `asyncio.Queue` loop.
*   **Concurrency:** Asynchronous non-blocking I/O (`aiohttp`). SCJN implements explicit `RateLimiter`.
*   **Coordination:** specialized Coordinator actors (`SCJNCoordinatorActor`, `BJVCoordinatorActor`) manage child actors (Discovery, Scraper, Persistence).

## 3. Data Sources & Persistence
*   **Sources:**
    *   **DOF:** `dof.gob.mx` (HTML parsing via `dof_parser`, `beautifulsoup4`).
    *   **SCJN:** (Likely API or specific search pages, `skip-pdfs` option implies PDF handling).
    *   **BJV:** Biblioteca Jurídica Virtual UNAM.
    *   **CAS:** Court of Arbitration for Sport.
*   **Persistence (The Vault):**
    *   **Files:** JSON files stored in `scraped_data/` (DOF), `scjn_data/`, `bjv_data/`.
    *   **Sanitization:** `PersistenceActor` (DOF) cleans filenames (`re.sub(r'[\/*?:\"<>|]', "", law.title)`).
    *   **Checkpoints:** `checkpoints/` directory stores session state (`*.json`) to allow resuming.

## 4. Job Lifecycle
*   **Discovery:** "Scouts" (`DofDiscoveryActor`, `SCJNDiscoveryActor`) find target URLs/Documents.
*   **Scraping:** "Workers" (`DofScraperActor`, `SCJNScraperActor`) fetch and parse content.
*   **Pipeline State:** Managed by Coordinators. States: `completed`, `error`, `failed`.
*   **Resilience:**
    *   **Checkpoints:** Save progress to disk.
    *   **Rate Limiting:** `RateLimiter` class (SCJN) with burst handling.
    *   **Graceful Shutdown:** Signal handlers for `SIGINT`/`SIGTERM`.

## 5. Configuration & Environment
*   **Dependencies:** `environment.yml` defines the stack (Python 3.10, `aiohttp`, `beautifulsoup4`, `lxml`, `fastapi`, `textual`).
*   **Config:** CLI arguments (`argparse`) used extensively for runtime config (`--max-results`, `--rate-limit`, `--output-dir`).

## 6. Key Identifiers for Diagrams
*   **Actors:** `SchedulerActor`, `DofScraperActor`, `PersistenceActor`, `DofDiscoveryActor`, `SCJNCoordinatorActor`, `BaseActor`.
*   **Messages:** `("SAVE_LAW", law)`, `("REGISTER_WORKER", worker)`, `("DISCOVER_RANGE", start, end)`, `DescubrirDocumentos`, `ObtenerEstado`.
*   **Directories:** `scraped_data`, `scjn_data`, `bjv_data`, `checkpoints`.
*   **External URLs:** `dof.gob.mx`, `nota_detalle.php`.
