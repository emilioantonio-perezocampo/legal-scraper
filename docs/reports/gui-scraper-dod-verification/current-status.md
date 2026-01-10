# Current Status: Web GUI ↔ Scraper Integration

## System Components
- **Frontend/API:** FastAPI (`src/gui/web/api.py`)
- **Backend:** `src.scjn_main` pipeline (Coordinator, Scraper, Persistence).
- **Entry Point:** `python -m src.gui.launcher --mode web`

## Integration Contract
### 1. Trigger
- **Endpoint:** `POST /api/scjn/start`
- **Payload:**
  ```json
  {
    "output_directory": "scjn_data",
    "max_results": 1,
    ...
  }
  ```

### 2. Artifact Generation
- **Directory:** `scjn_data/` (or configured `output_directory`).
- **Structure:**
  - `documents/{id}.json`
  - `embeddings/{id}.json`

### 3. File Serving
- **Base URL:** `/downloads/scjn` (mapped to `scjn_data` on disk).
- **Download URL:** `/downloads/scjn/documents/{filename}`.
