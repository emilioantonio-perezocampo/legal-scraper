import importlib
import sys
from typing import List, Tuple


# Core modules required for all deployments
REQUIRED_MODULES = [
    "aiohttp",
    "textual",
]

# Optional modules (GUI) - only needed for desktop deployments
OPTIONAL_MODULES = [
    "tkinter",  # Requires system X11 libs, skip in headless environments
]


def check_import(module_name: str) -> Tuple[bool, str]:
    try:
        importlib.import_module(module_name)
        return True, ""
    except ImportError as exc:
        return False, f"FAILED to import {module_name}: {exc}"
    except Exception as exc:
        return False, f"ERROR importing {module_name}: {exc}"


def run_smoke_test() -> int:
    print("🔥 Starting Container Smoke Test...")
    errors: List[str] = []

    # Check required modules
    for module_name in REQUIRED_MODULES:
        ok, error = check_import(module_name)
        if ok:
            print(f"✅ {module_name} imported successfully.")
        else:
            print(f"❌ {error}")
            errors.append(error)

    # Check optional modules (warn only)
    for module_name in OPTIONAL_MODULES:
        ok, error = check_import(module_name)
        if ok:
            print(f"✅ {module_name} imported successfully.")
        else:
            print(f"⚠️  {module_name} not available (optional for headless)")

    if errors:
        return 1

    print("🎉 ALL SYSTEMS GO. Container is ready for Hybrid operations.")
    return 0


def test_container_imports():
    """Test that required modules can be imported.

    Optional modules (like tkinter for GUI) are not checked here
    as they may not be available in headless server environments.
    """
    errors: List[str] = []
    for module_name in REQUIRED_MODULES:
        ok, error = check_import(module_name)
        if not ok:
            errors.append(error)
    assert not errors, " | ".join(errors)


if __name__ == "__main__":
    raise SystemExit(run_smoke_test())
