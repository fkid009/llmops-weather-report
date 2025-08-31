import os, json, time, logging, yaml
from pathlib import Path
from typing import Any, Dict, Optional

# -----------------------------
# Filesystem helpers
# -----------------------------

def save_json(
    obj: Any,
    path: str | Path,
    ensure_ascii: bool = False,
    indent: int = 2
) -> None:
    path = Path(path)
    with path.open("w", encoding = "utf-8") as f:
        json.dump(obj, f, ensure_ascii = ensure_ascii, indent = indent)

def load_json(path: str | Path) -> Any:
    path = Path(path)
    with path.open("r", encoding = "utf-8") as f:
        return json.load(f)
    
def load_yaml(path: str | Path) -> Dict[str, Any]:
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}