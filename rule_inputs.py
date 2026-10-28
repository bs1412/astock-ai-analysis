"""Content-addressed completed-rule inputs. Private evidence stays in account root."""
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile

import pandas as pd

ROOT = Path(__file__).resolve().parent / "rule_inputs"
COLUMNS = ("open", "high", "low", "close", "volume")


def _number(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError, OverflowError):
        return None


def evidence(code, frame, today, holding=None, persist=False, storage_root=None):
    rows = []
    attrs = frame.attrs if frame is not None else {}
    if frame is not None and not frame.empty:
        data = frame.copy()
        data["date"] = pd.to_datetime(data["date"], errors="coerce")
        data = data.loc[data["date"].notna() & data["date"].dt.date.lt(today)]
        data = data.drop_duplicates("date", keep="last").sort_values("date")
        for _, row in data.iterrows():
            rows.append([row["date"].isoformat(), *(_number(row.get(c)) for c in COLUMNS)])
    context = None if holding is None else {
        "cost_price": _number(holding.get("cost_price")),
        "acquired_on": str(holding.get("acquired_on") or "")}
    payload = {"schema": "completed_rule_input_v1", "code": str(code),
               "evaluation_date": today.isoformat(), "columns": ["date", *COLUMNS],
               "rows": rows, "holding_context": context,
               "source_contract": {key: attrs.get(key) for key in (
                   "source", "interval", "adjustment", "volume_unit",
                   "source_retrieved_at", "cache_generated_at")}}
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    identifier = hashlib.sha256(raw).hexdigest()
    if persist:
        folder = Path(storage_root) if storage_root is not None else ROOT
        folder.mkdir(exist_ok=True)
        destination = folder / (identifier + ".json")
        with tempfile.NamedTemporaryFile(dir=folder, prefix=".input-", suffix=".tmp", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(raw)
        try:
            try:
                os.link(temporary, destination)
            except FileExistsError:
                if destination.read_bytes() != raw:
                    raise ValueError("规则输入证据文件与内容标识不符，停止登记")
        finally:
            temporary.unlink(missing_ok=True)
    return {"schema": payload["schema"], "rule_input_id": identifier,
            "row_count": len(rows), "first_date": rows[0][0][:10] if rows else None,
            "last_date": rows[-1][0][:10] if rows else None,
            "evaluation_date": today.isoformat(), "holding_context_included": holding is not None,
            "stored": persist}
