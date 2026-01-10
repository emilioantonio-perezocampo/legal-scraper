import sys

print("🔥 Starting Container Smoke Test...")

def check_import(module_name):
    try:
        __import__(module_name)
        print(f"✅ {module_name} imported successfully.")
    except ImportError as e:
        print(f"❌ FAILED to import {module_name}: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ ERROR importing {module_name}: {e}")
        sys.exit(1)

# 1. Check Asyncio/Web (Core Logic)
check_import("aiohttp")

# 2. Check Textual (TUI Logic)
check_import("textual")

# 3. Check Tkinter (GUI Logic - The hardest one in Docker)
# Even if headless, it should at least be importable if libs are present.
check_import("tkinter")

print("🎉 ALL SYSTEMS GO. Container is ready for Hybrid operations.")
sys.exit(0)
