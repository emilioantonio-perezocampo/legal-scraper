# Diagram Index

**Total Diagrams:** 12 (Expanded High-Fidelity)
**Status:** All compiled and rendered.

## System Context
| Source | Rendered | Type | Purpose |
| :--- | :--- | :--- | :--- |
| `src/mermaid/c4_context.mmd` | `rendered/c4_context.svg` | Mermaid C4 | High-level context of Users, Scrapers, and External Sites. |

## Architecture & Topology
| Source | Rendered | Type | Purpose |
| :--- | :--- | :--- | :--- |
| `src/plantuml/architecture_components.puml` | `rendered/architecture_components.svg` | PlantUML Component | Detailed Actor hierarchy (`SCJNCoordinator`, `Discovery`) and Adapters. |
| `src/plantuml/deployment_topology.puml` | `rendered/deployment_topology.svg` | PlantUML Deployment | Runtime view (Docker, Volumes, Host). |

## Domain & Data
| Source | Rendered | Type | Purpose |
| :--- | :--- | :--- | :--- |
| `src/mermaid/domain_model.mmd` | `rendered/domain_model.svg` | Mermaid Class | Detailed Entities: `SCJNDocument`, `LibroBJV`, `LaudoArbitral`. |
| `src/mermaid/database_schema.mmd` | `rendered/database_schema.svg` | Mermaid ERD | Entity relationships and JSON schemas. |
| `src/plantuml/json_schema_dof.puml` | `rendered/json_schema_dof.svg` | PlantUML JSON | Visualization of the persisted JSON format. |

## Execution Flows
| Source | Rendered | Type | Purpose |
| :--- | :--- | :--- | :--- |
| `src/mermaid/scjn_pipeline_flow.mmd` | `rendered/scjn_pipeline_flow.svg` | Mermaid Flowchart | SCJN Logic: Discovery -> Dedup -> Queue -> Concurrency -> Download. |
| `src/mermaid/dof_pipeline_flow.mmd` | `rendered/dof_pipeline_flow.svg` | Mermaid Flowchart | Legacy DOF Pipeline Flow. |
| `src/mermaid/scjn_sequence.mmd` | `rendered/scjn_sequence.svg` | Mermaid Sequence | Detailed `Coordinator` <-> `Discovery` <-> `Scraper` interaction. |
| `src/mermaid/actor_messaging.mmd` | `rendered/actor_messaging.svg` | Mermaid Sequence | Async `tell`/`ask` mailbox pattern details. |
| `src/plantuml/discovery_logic.puml` | `rendered/discovery_logic.svg` | PlantUML Activity | Decision logic for Discovery filters. |

## State & Lifecycle
| Source | Rendered | Type | Purpose |
| :--- | :--- | :--- | :--- |
| `src/mermaid/pipeline_state.mmd` | `rendered/pipeline_state.svg` | Mermaid State | Exact `PipelineState` Enum transitions (IDLE -> DISCOVERING -> DOWNLOADING). |

## UI/UX
| Source | Rendered | Type | Purpose |
| :--- | :--- | :--- | :--- |
| `src/plantuml/tui_mockup.puml` | `rendered/tui_mockup.svg` | PlantUML Salt | Mockup of the Terminal User Interface. |
