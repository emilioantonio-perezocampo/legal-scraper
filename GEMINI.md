# 🧠 Project Context

## 1. Executive Summary

*   **Type:** Hybrid Asynchronous Scraper & Data Pipeline (CLI, TUI, GUI).
*   **Stack:** Python 3.10 (Conda), AsyncIO, Aiohttp, Tkinter/Textual (UI), Docker.
*   **Architecture:** Domain-Driven Design (DDD) integrated with an Actor Model. Data flows through a pipeline of independent actors: Discovery -> Scraper -> Parser -> Persistence.
*   **Status:** Active Task: Diagram Expansion (Complete). See `docs/architecture/diagrams/INDEX.md`.

## 2. Key Map

*   **Entry Point:** `src/main.py` (Headless/Actor Orchestration), `src/gui/launcher.py` (GUI).
*   **Core Modules:**
    *   `src/domain`: Immutable business logic (Entities, Value Objects, Repository Ports) using frozen dataclasses.
    *   `src/infrastructure/actors`: Async worker units (`DofScraperActor`, `PersistenceActor`) handling concurrency.
    *   `src/infrastructure/adapters`: Technical implementations like HTML parsers (`dof_parser.py`) and file system writers.
    *   `src/gui`: User interface layer supporting both Tkinter and Textual.
    *   `src/application`: (Currently Empty) Application logic is coordinated via Actor composition in `main.py`.

## 3. Developer Guidelines

*   **Conventions:**
    *   **DDD:** Strict separation of layers (`domain` depends on nothing).
    *   **Immutability:** Use `frozen=True` dataclasses for domain entities.
    *   **Concurrency:** Use `asyncio` and the internal Actor pattern; avoid threads where possible.
    *   **Typing:** Strict Python type hints are required.
*   **Testing:**
    *   Run unit/integration tests: `pytest`
    *   Container smoke test: `docker compose run --rm app python tests/container_smoke_test.py`

## 4. Diagram Syntax Reference (Mermaid & PlantUML)

### Mermaid.js
**Official Docs:** [https://mermaid.js.org/intro/](https://mermaid.js.org/intro/)

#### Flowchart
*   **Skeleton:**
    ```mermaid
    flowchart TD
      A[Start] --> B{Is Valid?}
      B -->|Yes| C[Process]
    ```

#### Sequence Diagram
*   **Skeleton:**
    ```mermaid
    sequenceDiagram
      participant A as Service
      participant B as Database
      A->>B: Query
    ```

#### State Diagram
*   **Skeleton:**
    ```mermaid
    stateDiagram-v2
      [*] --> Idle
      Idle --> Processing : Event
    ```

### PlantUML
**Official Docs:** [https://plantuml.com/](https://plantuml.com/)

#### C4 Context
*   **Skeleton:**
    ```plantuml
    !include https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Context.puml
    Person(user, "User")
    System(sys, "System")
    Rel(user, sys, "Uses")
    ```

#### Class Diagram
*   **Skeleton:**
    ```plantuml
    class MyClass {
      + attr: str
      + method(): void
    }
    ```
