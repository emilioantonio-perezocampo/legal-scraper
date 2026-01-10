# Capability Audit: Diagram Syntax Support

**Objective:** Enumerate ALL supported diagram types from official documentation and determine relevance for the Legal Scraper repository.

## 1. Mermaid.js
**Source:** [Official Mermaid Documentation](https://mermaid.js.org/intro/)

| Diagram Type | Syntax Key | Relevance | Planned File | Rationale |
|---|---|---|---|---|
| **Flowchart** | `graph` / `flowchart` | **CREATE** | `docs/architecture/diagrams/src/data_ingestion_flow.mmd` | Essential for visualizing the scraper -> parser -> storage pipeline. |
| **Sequence** | `sequenceDiagram` | **CREATE** | `docs/architecture/diagrams/src/job_execution_sequence.mmd` | Visualize actor message passing (Scheduler -> Discovery -> Scraper). |
| **Gantt** | `gantt` | **CREATE** | `docs/architecture/diagrams/src/historical_scrape_schedule.mmd` | Visualize the timeline for a full historical backfill job (estimated). |
| **Class** | `classDiagram` | **NOT RELEVANT** | - | PlantUML is preferred for strict UML Class diagrams in this repo. |
| **State** | `stateDiagram-v2` | **CREATE** | `docs/architecture/diagrams/src/scraper_actor_state.mmd` | Visualize the lifecycle (IDLE -> FETCHING -> PARSING) of an actor. |
| **ER Diagram** | `erDiagram` | **NOT RELEVANT** | - | We use JSON files, not a relational DB. No complex schema to map yet. |
| **User Journey** | `journey` | **CREATE** | `docs/architecture/diagrams/src/researcher_journey.mmd` | Map the UX of a legal researcher using the GUI to find laws. |
| **Git Graph** | `gitGraph` | **NOT RELEVANT** | - | Repo history is standard; no complex branching strategy to document. |
| **Pie Chart** | `pie` | **CREATE** | `docs/architecture/diagrams/src/document_distribution_pie.mmd` | Visualize the ratio of Federal vs State vs International docs (conceptual). |
| **Quadrant** | `quadrantChart` | **CREATE** | `docs/architecture/diagrams/src/source_complexity_quadrant.mmd` | Map sources (DOF, SCJN) by "Volume" vs "Scraping Difficulty". |
| **XY Chart** | `xychart` | **CREATE** | `docs/architecture/diagrams/src/daily_volume_trend.mmd` | Visualize documents published per day over a month (conceptual). |
| **Requirement** | `requirementDiagram` | **CREATE** | `docs/architecture/diagrams/src/system_requirements.mmd` | Trace high-level goals (Reliability, Speed) to Test verification. |
| **Mindmap** | `mindmap` | **CREATE** | `docs/architecture/diagrams/src/legal_domain_mindmap.mmd` | Visualize the taxonomy of Mexican Law (Constitution -> Federal -> State). |
| **Timeline** | `timeline` | **CREATE** | `docs/architecture/diagrams/src/project_roadmap.mmd` | High-level roadmap of the scraper project evolution. |
| **Sankey** | `sankey-beta` | **CREATE** | `docs/architecture/diagrams/src/data_throughput_sankey.mmd` | Visualize data flow volume from Source -> HTML -> Parsed -> JSON. |
| **C4 Context** | `C4Context` | **NOT RELEVANT** | - | PlantUML C4 is already established and preferred for this repo. |

## 2. PlantUML
**Source:** [Official PlantUML Documentation](https://plantuml.com/)

| Diagram Type | Syntax Key | Relevance | Planned File | Rationale |
|---|---|---|---|---|
| **Sequence** | `sequence` | **NOT RELEVANT** | - | Mermaid is preferred for simple flows; complex flows handled in Mermaid for this pass. |
| **Use Case** | `usecase` | **CREATE** | `docs/architecture/diagrams/src/user_interaction_usecase.puml` | Actors (Admin, Researcher) and their system interactions. |
| **Class** | `class` | **CREATE** | `docs/architecture/diagrams/src/domain_entities_class.puml` | Detailed Python dataclass structure (Immutable entities). |
| **Object** | `object` | **CREATE** | `docs/architecture/diagrams/src/runtime_objects_snapshot.puml` | Snapshot of Actor instances in memory during a job. |
| **Activity** | `activity` | **CREATE** | `docs/architecture/diagrams/src/pdf_parsing_logic.puml` | Complex decision tree for extracting text from PDFs (OCR vs Text). |
| **Component** | `component` | **CREATE** | `docs/architecture/diagrams/src/system_components.puml` | High-level structural modules (GUI, Infrastructure, Domain). |
| **Deployment** | `deployment` | **CREATE** | `docs/architecture/diagrams/src/docker_deployment.puml` | Docker container, volume mapping, and host OS relationship. |
| **State** | `state` | **NOT RELEVANT** | - | Mermaid state diagram is sufficient for the Actor lifecycle. |
| **Timing** | `timing` | **CREATE** | `docs/architecture/diagrams/src/rate_limit_timing.puml` | Visualize request spacing/throttling behavior (2s delay). |
| **Network** | `nwdiag` | **CREATE** | `docs/architecture/diagrams/src/network_topology.puml` | Interaction between Scraper container and External Gov Sites. |
| **Wireframe** | `salt` | **CREATE** | `docs/architecture/diagrams/src/tui_mockup.puml` | Mockup of the Terminal User Interface layout. |
| **Archimate** | `archimate` | **NOT RELEVANT** | - | Overkill for this project size. |
| **Gantt** | `gantt` | **NOT RELEVANT** | - | Mermaid Gantt is strictly better for web rendering. |
| **MindMap** | `mindmap` | **NOT RELEVANT** | - | Mermaid Mindmap is cleaner for docs. |
| **WBS** | `wbs` | **CREATE** | `docs/architecture/diagrams/src/task_breakdown_wbs.puml` | Breakdown of the "SCJN Extension" feature implementation. |
| **JSON** | `json` | **CREATE** | `docs/architecture/diagrams/src/scraped_data_schema.puml` | Visualization of the output JSON structure for a Law. |
| **YAML** | `yaml` | **NOT RELEVANT** | - | No complex YAML configs to document visually. |
| **Board** | `board` | **NOT RELEVANT** | - | Project management tool, not architectural diagram. |
