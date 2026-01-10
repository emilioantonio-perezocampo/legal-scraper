
## Deep Dive Expansion (High Fidelity)
**Date:** lunes, 5 de enero de 2026
**Command:** `python docs/architecture/diagrams/render_docs.py`

**Output:**
```
🚀 Starting Documentation Diagram Render Pipeline...
Found 41 diagrams to render.
...
☁️  [Kroki] Rendering scjn_pipeline_flow.mmd -> scjn_pipeline_flow.svg...
☁️  [Kroki] Rendering scjn_sequence.mmd -> scjn_sequence.svg...
----------------------------------------
🏁 Render Complete: 41 Success, 0 Failed
```

**Changes:**
- **Refactored `domain_model.mmd`:** Added full schema for `SCJNDocument`, `LibroBJV`, `LaudoArbitral` with exact attributes.
- **Refactored `architecture_components.puml`:** Detailed `infrastructure.actors` package with `SCJNCoordinator`, `Discovery`, `RateLimiter`.
- **Refactored `pipeline_state.mmd`:** Mapped exact `PipelineState` Enum values (`IDLE`, `DISCOVERING`, `DOWNLOADING`...).
- **Created `scjn_pipeline_flow.mmd`:** Detailed flowchart with RateLimit acquisition, Deduplication logic, and Concurrency checks.
- **Created `scjn_sequence.mmd`:** Step-by-step sequence of the Discovery->Coordinator->Scraper handshake.
