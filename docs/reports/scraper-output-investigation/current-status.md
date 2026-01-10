# Current Status: Scraper Output Investigation

## Expected Artifact Contract

Based on code analysis, the scraper is designed to produce the following artifacts:

### 1. File Formats
- **JSON (.json):** The primary output format for scraped laws, checkpoints, and documents.
- **PDF:** BJV scraper downloads PDFs (`bjv_pdf_actor.py`).

### 2. Output Locations
- **Default Directories:**
    - `scraped_data/` (Main generic output, used by `PersistenceActor`)
    - `cas_data/` (CAS scraper)
    - `bjv_data/` (BJV scraper)
    - `data/` (Default for base `PersistenceActor` if not overridden)
- **Configuration:**
    - `output_dir` argument in CLI scripts (`main.py`, `scjn_main.py`, `bjv_main.py`).
    - `CAS_OUTPUT_DIR` environment variable.

### 3. Naming Convention
- Laws: `{Title_snake_case}.json` (e.g., `Ley_de_Amparo.json`)
- Checkpoints: `checkpoint_{session_id}.json` or `{session_id}.json`
- Documents: `{document_id}.json`

## Observed Behavior
- Users report that "tests run but no output files are produced".
- Code analysis shows extensive use of `tmp_path` and temporary directories in tests, which are automatically cleaned up by `pytest`.

## Hypotheses
1.  **Tests use temporary directories (Most Likely):** The test suite (`pytest`) is correctly using isolated temporary directories (fixtures like `tmp_path`) for file I/O, which are deleted after tests finish. This is desired behavior for automated tests but confuses users expecting artifacts in the workspace.
2.  **Silent Failure:** The scrapers might be failing to write without raising exceptions, or exceptions are swallowed.
3.  **Mocking:** Some tests might be mocking the persistence layer entirely, avoiding file I/O.
