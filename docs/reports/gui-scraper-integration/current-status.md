# Current Status: GUI-Scraper Integration

## System Map
- **Entry Point:** `python -m src.gui.launcher --mode web`
- **Frontend/API:** FastAPI (Python)
- **Backend:** `src.gui.application.services.ScraperService` connecting to `DofScraperActor` / `DofDiscoveryActor`.
- **Scraper:** `src/scjn_main.py` (referenced in prompt, but `launcher.py` imports `DofScraperActor`? Need to verify which scraper is being used).

## Observation
`src/gui/main.py` imports `DofScraperActor`, not `SCJNScraperActor`.
But the user prompt mentioned `scjn_main.py` as the CLI example.
I need to clarify if the GUI supports SCJN or only DOF.
If the GUI *only* supports DOF, I must validate DOF.
If it supports both, I should check how to switch.

## Artifact Contract (Hypothesis)
- **Directory:** `scraped_data` (default in `GuiApplication`)
- **Format:** JSON
- **Download:** Likely via an API route (to be confirmed).
