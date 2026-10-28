"""Freeze public source identity at startup; never read configuration or data."""
import copy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re

from app_manifest import APP_FILES, APP_VERSION

ROOT = Path(__file__).resolve().parent
_LOADED = None


def disk_identity(root=ROOT):
    files = {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
             if (root / name).is_file() else None for name in APP_FILES}
    encoded = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    page = root / "web/index.html"
    match = re.search(r'class="ver">(V[0-9.]+)', page.read_text(encoding="utf-8")) if page.is_file() else None
    return {"code_sha256": hashlib.sha256(encoded).hexdigest(), "files": files,
            "page_version": match.group(1) if match else None,
            "missing_files": [name for name, value in files.items() if value is None]}


def initialize():
    global _LOADED
    if _LOADED is None:
        import rule_contract
        import trade_constraints
        _LOADED = {"app_version": APP_VERSION,
                   "started_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                   **disk_identity(), **rule_contract.metadata(),
                   "execution_policy_version": trade_constraints.POLICY_VERSION,
                   "identity_scope": "allowlisted_source_files_at_process_start"}


def status():
    if _LOADED is None:
        raise RuntimeError("runtime identity was not initialized at startup")
    loaded = copy.deepcopy(_LOADED)
    current = disk_identity()
    loaded["disk_matches_startup"] = current["code_sha256"] == loaded["code_sha256"]
    loaded["current_page_version"] = current["page_version"]
    loaded["current_missing_files"] = current["missing_files"]
    return loaded
