# Root Cause Analysis: No Output Files

## Diagnosis
**Case 1: Outputs are written to temporary directories and cleaned up.**

The investigation confirms that the scraper's test suite uses `pytest` fixtures (specifically `tmp_path`) to create isolated temporary directories for every test run.

## Evidence

1.  **Test Fixtures:**
    Files like `tests/actors/test_persistence.py` and `tests/integration/test_scjn_pipeline.py` explicitly request `tmp_path` from pytest and pass it to the actors/managers.
    
    ```python
    # tests/integration/test_scjn_pipeline.py
    @pytest.fixture
    def temp_output_dir(tmp_path):
        output_dir = tmp_path / "scjn_output"
        output_dir.mkdir()
        return output_dir
    ```

2.  **Pytest Behavior:**
    Pytest's `tmp_path` fixture creates a directory in the system's temp location (e.g., `/tmp/pytest-of-user/...` or `C:\Users\User\AppData\Local\Temp\pytest-...`). These are not located in the project workspace and are typically retained only for the last few runs, or cleaned up immediately depending on configuration.

3.  **No "Broken" Code:**
    The persistence logic is correctly implemented to write to the configured `output_dir`. In production (CLI usage), this defaults to `scraped_data/` or `scjn_data/`. In tests, this is overridden to be a temp dir.

## Conclusion
The observation "tests run but no output files are produced" is **correct and intended behavior** for the test suite. It prevents test artifacts from polluting the developer's workspace or git repository.

To verify the scraper's output capability, one should run the CLI application directly or use a reproduction script that explicitly targets a local directory.
