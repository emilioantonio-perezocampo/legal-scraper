import os
import subprocess
import sys
import zlib
import base64
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
print(f"   - Local PlantUML: {'✅' if CAN_RENDER_PLANTUML_LOCAL else '❌'}")
print(f"   - Local Mermaid: {'✅' if CAN_RENDER_MERMAID_LOCAL else '❌'}")

def render_via_kroki(file_path: Path, diagram_type: str) -> bool:
    """Render diagram using Kroki.io API."""
    out_file = DIAGRAMS_OUT / f"{file_path.stem}.svg"
    print(f"☁️  [Kroki] Rendering {file_path.name} -> {out_file.name}...")
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            source = f.read()

        # Kroki requires POST with the raw source
        url = f"{KROKI_API_URL}/{diagram_type}/svg"
        
        # Determine strictness based on type
        # C4-PlantUML often fails strict parsing, but standard PlantUML is robust
        
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

def render_plantuml_local(file_path: Path) -> bool:
    """Render PlantUML using local java jar (Not Implemented - Fallback to Kroki preferred)."""
    # Just forward to Kroki for simplicity/consistency in this env
    # unless user explicitly demands local.
    return render_via_kroki(file_path, "plantuml") # Also supports C4 via includes

def render_mermaid_local(file_path: Path) -> bool:
    """Render Mermaid using local mmdc."""
    out_file = DIAGRAMS_OUT / f"{file_path.stem}.svg"
    print(f"🖥️  [Local] Rendering {file_path.name} -> {out_file.name}...")
    
    cmd = ["mmdc", "-i", str(file_path), "-o", str(out_file)]
    try:
        subprocess.run(cmd, check=True)
        return True
    except Exception as e:
        print(f"❌ [Local] Failed: {e}")
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
            
        # Default to standard PlantUML
        return render_via_kroki(file_path, "plantuml")
        
    elif file_path.suffix == ".mmd":
        if CAN_RENDER_MERMAID_LOCAL:
            return render_mermaid_local(file_path)
        else:
            return render_via_kroki(file_path, "mermaid")
    return False

def main():
    print("🚀 Starting Production Diagram Render Pipeline...")
    ensure_dirs()
    
    success_count = 0
    fail_count = 0
    
    for file_path in DIAGRAMS_SRC.glob("*"):
        if file_path.suffix in [".puml", ".mmd"]:
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