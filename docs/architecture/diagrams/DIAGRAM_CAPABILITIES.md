# Diagram Capabilities Matrix

**Status:** Verified against official documentation.
**Date:** 2026-01-05

## A. Mermaid.js
**Official Docs:** [https://mermaid.js.org/intro/](https://mermaid.js.org/intro/)

| Diagram Type | Status | Justification / Plan |
| :--- | :--- | :--- |
| **Flowchart** | **YES** | **Core Logic.** Used for actor pipelines (Discovery -> Scraper -> Persistence). |
| **Sequence Diagram** | **YES** | **Async Messaging.** Visualizing `ask`/`tell` patterns between `Coordinator` and `Workers`. |
| **Class Diagram** | **YES** | **Domain Model.** `FederalLaw`, `Article` and their relationships. |
| **State Diagram** | **YES** | **Lifecycle.** Scraper pipeline states (Idle -> Running -> Paused -> Error). |
| **Entity Relationship** | **YES** | **Schema.** Relationships between Laws, Articles, and Metadata. |
| **Mindmap** | **YES** | **Taxonomy.** Hierarchy of handled legal areas (Federal, SCJN, BJV, CAS). |
| **Sankey** | **YES** | **Data Flow.** Conceptual flow of documents (Discovered -> Downloaded -> Filtered -> Saved). |
| **Gantt** | **NO** | Repository is a codebase, not a project schedule. No historical run data available to chart. |
| **Pie Chart** | **NO** | No static distribution data significant enough to warrant a diagram. |
| **Git Graph** | **NO** | Repo history is not relevant to architectural understanding. |
| **User Journey** | **NO** | System is primarily backend/CLI; user interaction is minimal. |
| **C4 Diagram** | **YES** | **Context.** High-level system context (User -> CLI -> Scrapers -> External Sites). |
| **Quadrant Chart** | **NO** | No data suitable for 2x2 matrix classification. |
| **Requirement Diagram** | **NO** | Requirements are not formally traced in the codebase. |
| **Timeline** | **NO** | No relevant chronological data to visualize. |
| **XY Chart** | **NO** | No statistical trends to plot. |

## B. PlantUML
**Official Docs:** [https://plantuml.com/](https://plantuml.com/)

| Diagram Type | Status | Justification / Plan |
| :--- | :--- | :--- |
| **C4 Context** | **YES** | **System Context.** Complementary to Mermaid C4, showing Container/Component levels. |
| **Class Diagram** | **YES** | **Detailed Design.** Python dataclasses and `BaseActor` inheritance hierarchy. |
| **Activity Diagram** | **YES** | **Logic.** Detailed decision tree for `DofDiscoveryActor` (Date vs Range vs All). |
| **Component Diagram** | **YES** | **Architecture.** Logical grouping of Scrapers, Actors, and Adapters. |
| **Deployment Diagram** | **YES** | **Topology.** Docker containers and volume mounts (`scraped_data`). |
| **Sequence Diagram** | **YES** | **Interactions.** Detailed `Coordinator` <-> `SessionManager` handshake. |
| **State Diagram** | **YES** | **Actor State.** `BaseActor` internal loop states. |
| **JSON Data** | **YES** | **Data Viz.** Visualizing the structure of the `FederalLaw` JSON output. |
| **Network (nwdiag)** | **YES** | **Topology.** Docker network visualization (if multiple containers exist). |
| **Wireframe (Salt)** | **YES** | **GUI Mockup.** Visualizing the `textual` TUI layout or FastAPI Web UI. |
| **MindMap** | **NO** | Redundant with Mermaid Mindmap (which is more web-native). |
| **WBS** | **NO** | Not a project management context. |
| **Gantt** | **NO** | Same reason as Mermaid. |
| **Timing Diagram** | **NO** | No strict real-time constraints or clock-cycle level logic. |
| **Object Diagram** | **NO** | Runtime object snapshots are less useful than Class diagrams here. |
| **YAML/EBNF/Regex** | **NO** | Not relevant for high-level architecture. |
