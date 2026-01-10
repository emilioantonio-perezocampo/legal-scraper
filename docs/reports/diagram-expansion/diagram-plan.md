# Diagram Plan: SCJN Scraper Expansion

**Strategy:** Maximize visibility into the system using the "Best Tool for the Job" approach.
- **Mermaid:** Used for flows, trends, timelines, and high-level conceptual maps.
- **PlantUML:** Used for strict architectural definitions, structures, and complex logic.

## 1. System Context & External Dependencies
| Diagram | Type | Purpose |
|---|---|---|
| `network_topology.puml` | PlantUML (nwdiag) | Shows how the Docker container connects to DOF/SCJN/BJV over HTTPS. |
| `source_complexity_quadrant.mmd` | Mermaid (Quadrant) | Conceptual mapping of source difficulty vs. data volume. |
| `scraped_data_schema.puml` | PlantUML (JSON) | Visual validation of the `FederalLaw` JSON export format. |

## 2. Logic & Execution Flow
| Diagram | Type | Purpose |
|---|---|---|
| `data_ingestion_flow.mmd` | Mermaid (Flowchart) | The "Main Pipeline": Discovery -> Scrape -> Parse -> Save. |
| `pdf_parsing_logic.puml` | PlantUML (Activity) | The internal logic of `PDFProcessorActor`: Extract Text -> Fallback to OCR. |
| `rate_limit_timing.puml` | PlantUML (Timing) | Visual proof of the "2-second cooldown" between requests. |
| `job_execution_sequence.mmd` | Mermaid (Sequence) | The message-passing dance between Scheduler, Bridge, and Actors. |

## 3. Data & Domain Structure
| Diagram | Type | Purpose |
|---|---|---|
| `domain_entities_class.puml` | PlantUML (Class) | Strict UML definition of `SCJNDocument`, `Article`, `Reform`. |
| `legal_domain_mindmap.mmd` | Mermaid (Mindmap) | Taxonomy of the business domain (Hierarchy of Mexican Laws). |
| `document_distribution_pie.mmd` | Mermaid (Pie) | Conceptual breakdown of the dataset by category (for stakeholders). |

## 4. Operational & Lifecycle
| Diagram | Type | Purpose |
|---|---|---|
| `scraper_actor_state.mmd` | Mermaid (State) | Lifecycle of a single worker actor (Idle/Busy/Error). |
| `runtime_objects_snapshot.puml` | PlantUML (Object) | What the `Scheduler` actually holds in memory at runtime. |
| `historical_scrape_schedule.mmd` | Mermaid (Gantt) | Estimated timeline for scraping 10 years of history. |
| `daily_volume_trend.mmd` | Mermaid (XYChart) | Expected data ingress volume over time. |
| `data_throughput_sankey.mmd` | Mermaid (Sankey) | Data loss visualization (Requests -> Valid HTML -> Parsed Articles). |

## 5. User Interaction & UX
| Diagram | Type | Purpose |
|---|---|---|
| `user_interaction_usecase.puml` | PlantUML (UseCase) | What users can actually DO (Start, Pause, Export). |
| `researcher_journey.mmd` | Mermaid (Journey) | The emotional/task journey of a user finding a specific law. |
| `tui_mockup.puml` | PlantUML (Salt) | Wireframe of the Terminal User Interface (TUI). |

## 6. Implementation & Roadmap
| Diagram | Type | Purpose |
|---|---|---|
| `task_breakdown_wbs.puml` | PlantUML (WBS) | Work breakdown for the SCJN extension task. |
| `project_roadmap.mmd` | Mermaid (Timeline) | Past achievements vs. Future goals. |
| `system_requirements.mmd` | Mermaid (Req) | Traceability from "Must scrape PDFs" to specific Actors. |
| `docker_deployment.puml` | PlantUML (Deployment) | The physical runtime environment. |
