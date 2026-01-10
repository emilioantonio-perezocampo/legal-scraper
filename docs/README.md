# Legal Scraper Architecture Documentation

## Overview
This directory contains the "Living Documentation" for the Legal Scraper project.
The diagrams are generated from code and serve as the source of truth for the system architecture.

## Structure
- `ARCHITECT.md`: High-level architectural overview, tech stack, and patterns.
- `decisions/`: Architectural Decision Records (ADRs).
- `diagrams/`: Visual representations of the system.
    - `src/`: Source files for diagrams (PlantUML, Mermaid).
    - `rendered/`: Generated SVG images.

## Diagrams

### 1. Component System (C4)
Visualizes the runtime architecture, specifically the Actor Model and GUI integration.
![Component System](diagrams/rendered/actor_component_system.svg)

### 2. Domain Class Diagram
Detailed UML class diagram of the Domain Layer, including SCJN entities.
![Domain Classes](diagrams/rendered/domain_class_diagram.svg)

### 3. Async Execution Flow
Sequence diagram tracing the critical path from GUI trigger to Actor execution.
![Execution Flow](diagrams/rendered/async_execution_flow.svg)

### 4. Actor Lifecycle
State machine diagram for the Scraper Actor.
![Actor State](diagrams/rendered/actor_lifecycle_state.svg)

### 5. Deployment View
Physical deployment topology (Docker/Host).
![Deployment](diagrams/rendered/deployment_view.svg)

## Building Diagrams
Run the render script to update diagrams after modifying source files:
```bash
python scripts/render_diagrams.py
```
