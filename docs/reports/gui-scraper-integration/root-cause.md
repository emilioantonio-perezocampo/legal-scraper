# Root Cause Analysis: GUI Integration Failure

## Issue 1: "Coordinator not connected"
The Web GUI (`src/gui/web/api.py`) initializes the `SCJNGuiBridgeActor` but fails to instantiate and connect the backend SCJN actors (`SCJNCoordinatorActor`, `SCJNDiscoveryActor`, etc.). This leaves the bridge with no destination for its commands.

## Issue 2: Missing Download Capability
The Web GUI has no API routes or static file mounts to serve the generated artifacts. Files are written to disk but are inaccessible via the web interface.

## Fix Plan

### 1. Initialize Backend Actors
In `src/gui/web/api.py`:
- Import `create_pipeline` (or equivalent logic) to spin up the SCJN actor system.
- In `startup()`, create the pipeline and pass the coordinator to `SCJNGuiBridgeActor`.
- In `shutdown()`, stop the coordinator.

### 2. Enable Downloads
In `src/gui/web/api.py`:
- Mount the default output directories (`scjn_data`, `bjv_data`) as static routes (e.g., `/downloads/scjn`).
- Ensure these directories exist at startup.

## Implementation Details
I will create a helper class `GuiScraperArgs` to mimic the `argparse` object expected by `create_pipeline`, allowing reuse of the existing wiring logic without code duplication.
