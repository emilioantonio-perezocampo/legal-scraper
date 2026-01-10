import os
import subprocess
import sys
import requests
from pathlib import Path

# Configuration
DIAGRAMS_SRC = Path("docs/architecture/diagrams/src")
DIAGRAMS_OUT = Path("docs/architecture/diagrams/rendered")
KROKI_API_URL = "https://kroki.io"

def ensure_dirs():
    """Ensure output directory exists."""
    DIAGRAMS_OUT.mkdir(parents=True, exist_ok=True)

def has_tool(command):
    """Check if a command line tool exists."""
    try:
        subprocess.run(command.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        return True
    except FileNotFoundError:
        return False

# Tool Availability Checks
HAS_JAVA = has_tool("java -version")
HAS_DOT = has_tool("dot -V")
HAS_MMDC = has_tool("mmdc --version")
# PlantUML Local requires Java + Graphviz (Dot)
CAN_RENDER_PLANTUML_LOCAL = HAS_JAVA and HAS_DOT 
# Mermaid Local requires Mermaid CLI
CAN_RENDER_MERMAID_LOCAL = HAS_MMDC

print(f"🔧 Environment Check:")
print(f"   - Java: {'✅' if HAS_JAVA else '❌'}")
print(f"   - Graphviz (dot): {'✅' if HAS_DOT else '❌'}")
print(f"   - Mermaid CLI (mmdc): {'✅' if HAS_MMDC else '❌'}")

def render_via_kroki(file_path: Path, diagram_type: str) -> bool:
    """Render diagram using Kroki.io API."""
    out_file = DIAGRAMS_OUT / f"{file_path.stem}.svg"
    print(f"☁️  [Kroki] Rendering {file_path.name} -> {out_file.name}...")
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            source = f.read()

        # Kroki requires POST with the raw source
        url = f"{KROKI_API_URL}/{diagram_type}/svg"
        
        response = requests.post(
            url, 
            data=source.encode('utf-8'),
            headers={"Content-Type": "text/plain"},
            timeout=30 # 30s timeout
        )
        
        if response.status_code == 200:
            with open(out_file, "wb") as f:
                f.write(response.content)
            return True
        else:
            print(f"❌ [Kroki] Error {response.status_code}: {response.text[:200]}")
            return False

    except Exception as e:
        print(f"❌ [Kroki] Exception: {e}")
        return False

def render_diagram(file_path: Path) -> bool:
    """Dispatch render based on file extension and availability."""
    if file_path.suffix == ".puml":
        # Check for C4 content to use specific renderer
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                if "!include" in content and "C4" in content:
                    return render_via_kroki(file_path, "c4plantuml")
        except Exception:
            pass
            
        return render_via_kroki(file_path, "plantuml")
        
    elif file_path.suffix == ".mmd":
        return render_via_kroki(file_path, "mermaid")
    return False

def main():
    print("🚀 Starting Documentation Diagram Render Pipeline...")
    ensure_dirs()
    
    success_count = 0
    fail_count = 0
    
    # RECURSIVE GLOB FIXED HERE
    files = list(DIAGRAMS_SRC.rglob("*.puml")) + list(DIAGRAMS_SRC.rglob("*.mmd"))
    
    print(f"Found {len(files)} diagrams to render.")

    for file_path in files:
        if render_diagram(file_path):
            success_count += 1
        else:
            fail_count += 1
                
    print("-" * 40)
    print(f"🏁 Render Complete: {success_count} Success, {fail_count} Failed")
    
    if fail_count > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
