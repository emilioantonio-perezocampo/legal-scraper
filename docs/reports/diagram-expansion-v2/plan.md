# Diagram Expansion Plan

**Objective:** Create a comprehensive, repo-grounded diagram suite.
**Basis:** `DIAGRAM_CAPABILITIES.md` (YES only).

## 1. System Context
*   **Diagram:** `c4_context.mmd`
*   **Type:** Mermaid C4
*   **Purpose:** High-level view of the system and external actors.
*   **Traceability:**
    *   **User:** CLI Operator.
    *   **System:** `Legal Scraper`.
    *   **Containers:** `DOF Scraper`, `SCJN Scraper`, `BJV Scraper`, `GUI`.
    *   **External:** `dof.gob.mx`, `SCJN Portal`, `BJV UNAM`.
    *   **Files:** `src/main.py`, `src/scjn_main.py`, `src/bjv_main.py`.

## 2. Architecture & Components
*   **Diagram:** `architecture_components.puml`
*   **Type:** PlantUML Component
*   **Purpose:** Logical organization of the codebase.
*   **Traceability:**
    *   **Modules:** `src.infrastructure`, `src.domain`, `src.application`, `src.gui`.
    *   **Components:** `ActorSystem`, `PersistenceLayer`, `Adapters`.
    *   **Files:** `src/infrastructure/actors/`, `src/gui/`.

*   **Diagram:** `deployment_topology.puml`
*   **Type:** PlantUML Deployment
*   **Purpose:** Physical/Runtime view.
*   **Traceability:**
    *   **Nodes:** `Docker Container`, `Host FileSystem`.
    *   **Artifacts:** `scraped_data/`, `environment.yml`.
    *   **Files:** `Dockerfile`, `docker-compose.yml`.

## 3. Domain & Data
*   **Diagram:** `domain_model.mmd`
*   **Type:** Mermaid Class
*   **Purpose:** Core entity relationships.
*   **Traceability:**
    *   **Classes:** `FederalLaw`, `Article`.
    *   **Attributes:** `title`, `publication_date`, `jurisdiction`.
    *   **Files:** `src/domain/entities.py`, `src/infrastructure/actors/persistence.py`.

*   **Diagram:** `json_schema_dof.puml`
*   **Type:** PlantUML JSON
*   **Purpose:** Visualization of the persistence format.
*   **Traceability:**
    *   **Structure:** JSON output format from `PersistenceActor._save_to_json`.
    *   **Files:** `src/infrastructure/actors/persistence.py`.

## 4. Execution Flows (Logic)
*   **Diagram:** `dof_pipeline_flow.mmd`
*   **Type:** Mermaid Flowchart
*   **Purpose:** Data flow through the DOF actors.
*   **Traceability:**
    *   **Nodes:** `DofDiscoveryActor`, `DofScraperActor`, `PersistenceActor`.
    *   **Flow:** Discovery -> Scraping -> Saving.
    *   **Files:** `src/main.py` (wiring logic).

*   **Diagram:** `actor_messaging.mmd`
*   **Type:** Mermaid Sequence
*   **Purpose:** Async message patterns.
*   **Traceability:**
    *   **Methods:** `tell()`, `ask()`, `handle_message()`.
    *   **Messages:** `SAVE_LAW`, `REGISTER_WORKER`.
    *   **Files:** `src/infrastructure/actors/base.py`.

*   **Diagram:** `discovery_logic.puml`
*   **Type:** PlantUML Activity
*   **Purpose:** Decision logic for discovery.
*   **Traceability:**
    *   **Logic:** Date Range vs Single Date vs Status.
    *   **Files:** `src/scjn_main.py` (`run_discovery`), `src/infrastructure/actors/dof_discovery_actor.py`.

## 5. State & Lifecycle
*   **Diagram:** `pipeline_state.mmd`
*   **Type:** Mermaid State
*   **Purpose:** Lifecycle of the SCJN Coordinator.
*   **Traceability:**
    *   **States:** `Idle`, `Discovering`, `Downloading`, `Paused`, `Completed`.
    *   **Files:** `src/scjn_main.py` (status loop).

## 6. UI/UX
*   **Diagram:** `tui_mockup.puml`
*   **Type:** PlantUML Salt
*   **Purpose:** Conceptual layout of the Terminal UI.
*   **Traceability:**
    *   **Components:** `Launcher`, `StatusPanel`, `LogWindow`.
    *   **Files:** `src/gui/tui/app.py` (inferred from file list and deps).
