# Fix Plan: Web GUI Integration

## Objective
Enable the Web GUI to successfully trigger SCJN scrapes and serve the resulting files.

## Changes to `src/gui/web/api.py`

1.  **Imports:**
    - `from src.scjn_main import create_pipeline, stop_pipeline`
    - `from dataclasses import dataclass`

2.  **Helper Class:**
    ```python
    @dataclass
    class ScraperArgs:
        output_dir: str = "scjn_data"
        checkpoint_dir: str = "checkpoints"
        rate_limit: float = 0.5
        concurrency: int = 3
        skip_pdfs: bool = True
        max_results: int = 100
        # Optional filters
        category: str = None
        scope: str = None
        status: str = None
        all_pages: bool = False
    ```

3.  **Update `ScraperAPI` class:**
    - Add `self._scjn_coordinator = None`
    - Update `startup()`:
        ```python
        # Initialize SCJN pipeline
        args = ScraperArgs()
        self._scjn_coordinator = await create_pipeline(args)
        
        self._scjn_bridge = SCJNGuiBridgeActor(
            coordinator_actor=self._scjn_coordinator,
            event_handler=self._handle_scjn_event
        )
        await self._scjn_bridge.start()
        ```
    - Update `shutdown()`:
        ```python
        if self._scjn_coordinator:
            await stop_pipeline(self._scjn_coordinator)
        ```

4.  **Update `create_app` function:**
    - Create output directories if they don't exist.
    - Mount them:
        ```python
        app.mount("/downloads/scjn", StaticFiles(directory="scjn_data"), name="scjn_downloads")
        ```

## Validation
1.  Restart Web GUI.
2.  Trigger SCJN scrape (using default `scjn_data` to match mount).
3.  Verify status flows (IDLE -> RUNNING -> IDLE).
4.  Verify file created in `scjn_data`.
5.  Verify download via `http://localhost:8000/downloads/scjn/documents/{id}.json`.
