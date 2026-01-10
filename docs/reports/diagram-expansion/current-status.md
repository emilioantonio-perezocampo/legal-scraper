# Current Status: Diagram Expansion

## 1. Repo Diagram Inventory
Location: `docs/architecture/diagrams/src/`

| File | Type | Purpose |
|------|------|---------|
| `actor_component_system.puml` | PlantUML (C4) | Runtime architecture, Actor/GUI integration |
| `actor_lifecycle_state.mmd` | Mermaid | State machine for Scraper Actor |
| `async_execution_flow.mmd` | Mermaid | Sequence: GUI Trigger -> Actor execution |
| `deployment_view.puml` | PlantUML | Docker/Host deployment topology |
| `domain_class_diagram.puml` | PlantUML | Class diagram for Domain Layer (SCJN/Legacy) |
| `system_context.puml` | PlantUML (C4) | System Context (Level 1) |

## 2. Compilation Pipeline
- **Script**: `scripts/render_diagrams.py`
- **Logic**:
  - Checks for local tools (`java`, `dot`, `mmdc`).
  - Falls back to `https://kroki.io` API if local tools missing.
  - Supports `.puml` (PlantUML) and `.mmd` (Mermaid).
  - Output: `docs/architecture/diagrams/rendered/*.svg`

## 3. Gaps & Opportunities
- **Data Flow**: No specific data flow diagram (ETL specifics).
- **Concurrency**: Only one sequence diagram; might need more detailed actor interaction views.
- **Error Handling**: Covered in sequence diagram, but maybe a dedicated failure mode view is useful.
- **Observability**: No diagrams for logging/monitoring flow.
- **Missing Types**:
  - **Mermaid**: Entity Relationship (ER) for database? Gantt/Timeline for job scheduling? Mindmap for domain concepts?
  - **PlantUML**: Activity diagrams for complex algorithms (e.g., PDF parsing logic)?
