"""Optional process-shared public OHLCV cache. No account records or secrets."""
from collections import Counter
from contextlib import contextmanager
from datetime import datetime
import hashlib
import json
import math
import numbers
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
import uuid

import pandas as pd

SUCCESS_SECONDS = 300
FAILURE_SECONDS = 30
LEASE_SECONDS = 90
WAIT_SECONDS = 60
MAX_ROWS = 256
MAX_PAYLOAD_BYTES = 2_000_000
ATTRS = ("source", "interval", "adjustment", "volume_unit", "source_retrieved_at", "cache_generated_at",
         "provider_price_fields", "provider_http_requests", "provider_adapter")
COLUMNS = ("date", "open", "high", "low", "close", "volume", "amount")
_LOCK = threading.Lock()
_METRICS = Counter()


def _count(key):
    with _LOCK:
        _METRICS[key] += 1


def diagnostics():
    with _LOCK:
        return {"enabled": bool(os.getenv("PUBLIC_DAILY_CACHE_DB")), "counts": dict(_METRICS),
                "success_ttl_seconds": SUCCESS_SECONDS, "failure_ttl_seconds": FAILURE_SECONDS,
                "wait_seconds": WAIT_SECONDS, "max_records": MAX_ROWS,
                "scope": "public_daily_only_no_account_data"}


@contextmanager
def _database(path):
    connection = sqlite3.connect(path, timeout=2)
    try:
        connection.execute("CREATE TABLE IF NOT EXISTS public_daily_v1 (cache_key TEXT PRIMARY KEY, payload TEXT, checksum TEXT, created REAL, ttl REAL, owner TEXT, lease_until REAL)")
        connection.commit()
        yield connection
    finally:
        connection.close()


def _encode(frame):
    if frame is None or frame.empty:
        value = None
    else:
        columns = [name for name in COLUMNS if name in frame]
        records = []
        for _, row in frame.iterrows():
            record = []
            for column in columns:
                raw = row[column]
                if column == "date":
                    record.append(pd.Timestamp(raw).isoformat() if pd.notna(raw) else None)
                else:
                    number = raw.item() if hasattr(raw, "item") else raw
                    if number is None or pd.isna(number):
                        record.append(None)
                    elif isinstance(number, str):
                        record.append(number)
                    elif isinstance(number, numbers.Integral):
                        record.append(int(number))
                    else:
                        record.append(float(number) if math.isfinite(float(number)) else None)
            records.append(record)
        value = {"columns": columns, "rows": records,
                 "attrs": {name: frame.attrs.get(name) for name in ATTRS}}
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(payload.encode("utf-8")) > MAX_PAYLOAD_BYTES:
        raise ValueError("public frame exceeds cache payload limit")
    return payload, hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _decode(payload, checksum):
    if hashlib.sha256(payload.encode("utf-8")).hexdigest() != checksum:
        raise ValueError("public cache checksum mismatch")
    value = json.loads(payload)
    if value is None:
        return None
    if (not isinstance(value, dict) or set(value) != {"columns", "rows", "attrs"}
            or not isinstance(value["columns"], list) or len(set(value["columns"])) != len(value["columns"])
            or any(name not in COLUMNS for name in value["columns"])
            or any(name not in ATTRS for name in value["attrs"])):
        raise ValueError("invalid public cache schema")
    frame = pd.DataFrame(value["rows"], columns=value["columns"])
    if "date" not in frame or any(name not in frame for name in ("open", "high", "low", "close", "volume")):
        raise ValueError("missing public OHLCV columns")
    frame["date"] = pd.to_datetime(frame["date"], errors="raise")
    frame.attrs.update(value["attrs"])
    frame.attrs["request_cache_state"] = "shared_public"
    return frame


def _publish(path, key, owner, frame):
    payload, checksum = _encode(frame)
    ttl = FAILURE_SECONDS if frame is None or frame.empty else SUCCESS_SECONDS
    with _database(path) as db:
        with db:
            db.execute("UPDATE public_daily_v1 SET payload=?,checksum=?,created=?,ttl=?,owner=NULL,lease_until=0 WHERE cache_key=? AND owner=?", (payload,checksum,time.time(),ttl,key,owner))
            db.execute("DELETE FROM public_daily_v1 WHERE owner IS NOT NULL AND lease_until<=?", (time.time(),))
            db.execute("DELETE FROM public_daily_v1 WHERE owner IS NULL AND cache_key IN (SELECT cache_key FROM public_daily_v1 WHERE owner IS NULL ORDER BY created DESC LIMIT -1 OFFSET ?)", (MAX_ROWS,))


def daily(code, start, end, fetch, namespace="fast_tx_sina_em_v218", minimum_remaining_seconds=0):
    if not isinstance(minimum_remaining_seconds,numbers.Real) or not math.isfinite(minimum_remaining_seconds) or not 0<=minimum_remaining_seconds<SUCCESS_SECONDS:
        raise ValueError("invalid public cache remaining lifetime")
    configured = os.getenv("PUBLIC_DAILY_CACHE_DB", "")
    if not configured:
        return fetch()
    code, start, end = str(code), str(start), str(end)
    if not re.fullmatch(r"\d{6}", code) or not re.fullmatch(r"\d{8}", start) or not re.fullmatch(r"\d{8}", end):
        raise ValueError("invalid public daily request")
    first, last = datetime.strptime(start, "%Y%m%d"), datetime.strptime(end, "%Y%m%d")
    if not 0 <= (last-first).days <= 10000 or namespace not in (
            "fast_tx_sina_em_v218", "secondary_sina_v218", "secondary_tencent_v218", "secondary_eastmoney_v218",
            "fast_bj920_v222", "secondary_sina_bj920_v222", "secondary_tencent_bj920_v222", "secondary_eastmoney_bj920_v222",
            "fast_sh689_receipts_v222", "secondary_sina_sh689_receipts_v222", "secondary_tencent_sh689_receipts_v222", "secondary_eastmoney_sh689_receipts_v222",
            "fast_tx_sina_em_v224", "secondary_sina_v224", "secondary_tencent_v224", "secondary_eastmoney_v224",
            "fast_bj920_sina_v224", "secondary_sina_bj920_sina_v224", "secondary_tencent_bj920_sina_v224", "secondary_eastmoney_bj920_sina_v224",
            "fast_sh689_receipts_sina_v224", "secondary_sina_sh689_receipts_sina_v224", "secondary_tencent_sh689_receipts_sina_v224", "secondary_eastmoney_sh689_receipts_sina_v224",
            "research_raw_tencent_v219", "research_qfq_tencent_v219"):
        raise ValueError("invalid public daily range or contract")
    path = Path(configured)
    key = hashlib.sha256(json.dumps([namespace, code, start, end], separators=(",", ":")).encode()).hexdigest()
    owner = uuid.uuid4().hex
    deadline = time.monotonic() + WAIT_SECONDS
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        while True:
            with _database(path) as db:
                db.execute("BEGIN IMMEDIATE")
                # Lock acquisition can wait. Compare lease/TTL against time AFTER
                # acquiring it, or a fresh owner can look implausibly in the future.
                now = time.time()
                row = db.execute("SELECT payload,checksum,created,ttl,owner,lease_until FROM public_daily_v1 WHERE cache_key=?", (key,)).fetchone()
                remaining_guard=minimum_remaining_seconds if row and row[3]==SUCCESS_SECONDS else 0
                if row and row[0] is not None and 0 <= now-row[2] < row[3]-remaining_guard:
                    try:
                        frame = _decode(row[0], row[1])
                    except (ValueError, TypeError, KeyError):
                        _count("invalidated_content")
                    else:
                        db.commit()
                        _count("shared_hit" if frame is not None else "shared_failure_hit")
                        return frame
                live = row and row[4] and 0 < row[5]-now <= LEASE_SECONDS+1
                if not live:
                    db.execute("INSERT OR REPLACE INTO public_daily_v1 VALUES (?,?,?,0,0,?,?)", (key, None, None, owner, now+LEASE_SECONDS))
                    db.commit()
                    break
                db.commit()
            if time.monotonic() >= deadline:
                _count("wait_timeout")
                return None
            _count("wait_poll")
            time.sleep(0.05)
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError):
        _count("cache_unavailable_bypass")
        return fetch()
    _count("leader_fetch")
    try:
        try:
            frame = fetch()
        except Exception:
            _count("provider_exception")
            try:
                _publish(path, key, owner, None)
            except (OSError, sqlite3.Error, ValueError, TypeError):
                _count("cache_publish_failed")
            raise
        try:
            _publish(path, key, owner, frame)
        except (OSError, sqlite3.Error, ValueError, TypeError):
            _count("cache_publish_failed")
        return frame
    finally:
        try:
            with _database(path) as db:
                with db:
                    db.execute("DELETE FROM public_daily_v1 WHERE cache_key=? AND owner=?", (key,owner))
        except (OSError, sqlite3.Error):
            _count("lease_cleanup_failed")
