# -*- coding: utf-8 -*-
"""智能筛选：全市场并发扫描(腾讯/新浪) + 每码因子缓存 + 规则过滤排名。"""
import json
import os
import tempfile
import threading
import concurrent.futures
import copy
import time
from datetime import date, datetime

import numpy as np
import pandas as pd
import stock_agent as sa
import entry_signals
import market_clock

UNIVERSE = [
    "600519", "600036", "601318", "600900", "601899", "600030", "600276", "603259",
    "600887", "600309", "601012", "600809", "601088", "600028", "601857", "601398",
    "601288", "601988", "600000", "600016", "600031", "600585", "600690", "601668",
    "600104", "600018", "601601", "601628", "601166", "600999", "600050", "600893",
    "000001", "000333", "000858", "000651", "000725", "002415", "002594", "002475",
    "002714", "300750", "300059", "300015", "300124", "002230", "002352", "000063",
    "002027", "000568", "000002", "002304", "300122", "002142", "000776", "002493",
    "688981", "688111", "688012", "300760", "300308",
]

_BASE = os.path.dirname(os.path.abspath(__file__))
_CACHE_DIR = os.path.join(_BASE, "factor_cache")
_SCAN_FILE = os.path.join(_BASE, "scan_state.json")
_CACHE_SCHEMA = 6
VOLUME_LABELS = {"shares": "股", "depositary_receipts": "份（存托凭证）"}
MAX_RANK_QUOTE_AGE_DAYS = entry_signals.MAX_QUOTE_AGE_DAYS
os.makedirs(_CACHE_DIR, exist_ok=True)
_SCAN = {"date": None, "running": False, "total": 0, "done": 0, "succeeded": 0, "failed": 0,
         "started_at": None, "finished_at": None, "error": None, "message": "尚未运行",
         "stage": "idle", "interrupted": False, "failed_by_code": {}, "state_error": None,
         "prepare_ms": None, "execute_ms": None, "total_ms": None, "worker_count": None}
_SCAN_LOCK = threading.Lock()
_SCAN_SAVE_LOCK = threading.Lock()
_SCAN_START_LOCK = threading.Lock()
_TODAY_ROWS = {"at": 0.0, "date": None, "rows": []}
_TODAY_ROWS_LOCK = threading.Lock()
_FACTOR_DIAGNOSTICS = threading.local()


def _cache_path(code):
    return os.path.join(_CACHE_DIR, f"{code}.json")


def _load_code_cache(code):
    try:
        with open(_cache_path(code), encoding="utf-8") as f:
            value = json.load(f)
        return value if isinstance(value, dict) else None
    except Exception:
        return None


def _save_code_cache(code, data):
    path = _cache_path(code)
    fd, temp_path = tempfile.mkstemp(prefix=f".{code}-", suffix=".tmp", dir=_CACHE_DIR)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass


def _normalize_date(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if not float(value).is_integer():
            return None
        value = str(int(value))
    raw = str(value).strip()
    # Provider/cached dates overwhelmingly use these forms. Avoid invoking the
    # pandas scalar format-inference stack for every bar/cache file.
    if isinstance(value, (datetime, date)):
        return value.date().isoformat() if isinstance(value, datetime) else value.isoformat()
    if (len(raw) == 8 and raw.isascii() and raw.isdigit()) or (len(raw) == 10 and raw[4] == "-" and raw[7] == "-"):
        try:
            return date.fromisoformat(raw).isoformat()
        except ValueError:
            return None
    if len(raw) == 8 and raw.isdigit():
        parsed = pd.to_datetime(raw, format="%Y%m%d", errors="coerce")
    else:
        parsed = pd.to_datetime(raw, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.strftime("%Y-%m-%d")


def _cache_is_current(value, today):
    data = value.get("data") if isinstance(value, dict) else None
    as_of = _normalize_date(data.get("as_of")) if isinstance(data, dict) else None
    return bool(value and value.get("schema") == _CACHE_SCHEMA and value.get("date") == today
                and isinstance(data, dict) and as_of and as_of <= today
                and isinstance(data.get("data_quality"), list)
                and "data_source" in data and data.get("cache_generated_at")
                and data.get("volume_unit") in VOLUME_LABELS.values())


def _scan_state_payload():
    with _SCAN_LOCK:
        return copy.deepcopy({key: value for key, value in _SCAN.items() if key != "state_error"})


def _persist_scan_state():
    with _SCAN_SAVE_LOCK:
        payload = _scan_state_payload()
        temp_path = None
        try:
            fd, temp_path = tempfile.mkstemp(prefix=".scan-state-", suffix=".tmp", dir=_BASE)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            os.replace(temp_path, _SCAN_FILE)
            with _SCAN_LOCK:
                _SCAN["state_error"] = None
            return True
        except Exception as e:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            with _SCAN_LOCK:
                _SCAN["state_error"] = f"扫描进度文件保存失败：{str(e)[:160]}"
            return False


def _restore_scan_state():
    try:
        with open(_SCAN_FILE, encoding="utf-8") as f:
            saved = json.load(f)
    except (OSError, ValueError, TypeError):
        return
    today = market_clock.now().strftime("%Y-%m-%d")
    if not isinstance(saved, dict) or saved.get("date") != today:
        return
    fields = ("date", "total", "done", "succeeded", "failed", "started_at", "finished_at",
              "error", "message", "stage", "interrupted", "prepare_ms", "execute_ms",
              "total_ms", "worker_count")
    with _SCAN_LOCK:
        for key in fields:
            if key in saved:
                _SCAN[key] = saved[key]
        saved_failures = saved.get("failed_by_code", {})
        _SCAN["failed_by_code"] = {str(code): value for code, value in saved_failures.items()
                                    if isinstance(value, dict) and isinstance(code, str)} if isinstance(saved_failures, dict) else {}
        was_running = bool(saved.get("running"))
        _SCAN["running"] = False
        if was_running:
            _SCAN["interrupted"] = True
            _SCAN["stage"] = "interrupted"
            _SCAN["message"] = "上次扫描进程中断；重新开始会跳过有效缓存并重试未完成股票"
    if was_running:
        _persist_scan_state()


_restore_scan_state()


def _factors(code, refresh=False):
    _FACTOR_DIAGNOSTICS.value = None
    today = market_clock.now().strftime("%Y-%m-%d")
    hit = _load_code_cache(code)
    if not refresh and _cache_is_current(hit, today):
        return hit["data"]
    current = market_clock.now()
    end = current.strftime("%Y%m%d")
    start = (current - pd.Timedelta(days=400)).strftime("%Y%m%d")
    df = sa._kline_fast(code, start, end)
    if df is None or df.empty:
        _FACTOR_DIAGNOSTICS.value = {"kind": "no_usable_data", "available_rows": 0,
                                     "required_rows": 25, "error": "供应源未返回可用日线；不能生成因子缓存"}
        return None
    source = getattr(df, "attrs", {}).get("source")
    source_retrieved_at = getattr(df, "attrs", {}).get("source_retrieved_at")
    volume_unit = VOLUME_LABELS.get(getattr(df, "attrs", {}).get("volume_unit"), "未知")
    df = df.copy()
    required = ("date", "open", "close", "high", "low", "volume")
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError("行情数据缺少字段：" + ", ".join(missing))
    df["date"] = df["date"].map(_normalize_date)
    numeric_fields = ("open", "close", "high", "low", "volume")
    for c in numeric_fields:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    values = df[list(numeric_fields)].to_numpy(dtype=float)
    finite = pd.Series(np.isfinite(values).all(axis=1), index=df.index)
    price_valid = ((df["open"] > 0) & (df["close"] > 0) & (df["high"] > 0) & (df["low"] > 0)
                   & (df["high"] >= df["low"])
                   & (df["low"] <= df["open"]) & (df["open"] <= df["high"])
                   & (df["low"] <= df["close"]) & (df["close"] <= df["high"])
                   & (df["volume"] >= 0))
    valid = df["date"].notna() & finite & price_valid
    invalid_bar_count = int((~valid).sum())
    df = df.loc[valid].copy()
    duplicate_bar_count = int(df.duplicated(subset=["date"], keep="last").sum())
    df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
    if len(df) < 25:
        _FACTOR_DIAGNOSTICS.value = {"kind": "insufficient_history", "available_rows": len(df),
                                     "required_rows": 25, "invalid_rows": invalid_bar_count,
                                     "duplicate_rows": duplicate_bar_count,
                                     "error": f"有效历史不足25根日线：实际{len(df)}根；不能生成因子缓存"}
        return None
    as_of = df["date"].iloc[-1]
    if as_of > today:
        raise ValueError("行情数据日期晚于当前日期，拒绝写入缓存")
    cache_generated_at = market_clock.now().astimezone().isoformat(timespec="seconds")
    data_quality = []
    if invalid_bar_count:
        data_quality.append(f"剔除异常或缺字段日线 {invalid_bar_count} 条")
    if duplicate_bar_count:
        data_quality.append(f"重复日期日线去重，丢弃 {duplicate_bar_count} 条")
    if float(df["volume"].iloc[-1]) == 0:
        data_quality.append("最新日线成交量为0；可能停牌或数据源缺失，原因未核实")
    if source not in ("tencent", "sina", "eastmoney"):
        data_quality.append("行情供应源未知")
    if volume_unit == "未知":
        data_quality.append("成交量单位未核实")
    if volume_unit == "份（存托凭证）":
        data_quality.append("存托凭证以份计量；模拟交易数量/权限政策未登记")
    kline = [{"date": str(r["date"]), "open": round(float(r["open"]), 3), "close": round(float(r["close"]), 3),
              "high": round(float(r["high"]), 3), "low": round(float(r["low"]), 3), "volume": float(r["volume"])}
             for _, r in df.tail(60).iterrows()]
    tech = sa.calc_technical_indicators(kline)
    close, high, low = df["close"], df["high"], df["low"]
    mom20 = (close.iloc[-1] / close.iloc[-21] - 1) * 100 if len(close) > 21 else None
    h60, l60 = high.tail(60).max(), low.tail(60).min()
    rng = (close.iloc[-1] - l60) / (h60 - l60) if h60 != l60 else 0.5
    f = tech["factors"]
    fv = {
        "code": code,
        "as_of": as_of,
        "data_source": source,
        "volume_unit": volume_unit,
        "source_retrieved_at": source_retrieved_at,
        "cache_generated_at": cache_generated_at,
        "bar_count": len(df),
        "invalid_bar_count": invalid_bar_count,
        "duplicate_bar_count": duplicate_bar_count,
        "latest_volume": float(df["volume"].iloc[-1]),
        "data_quality": data_quality,
        "close": tech["latest"]["close"],
        "rsi": tech["latest"]["RSI14"],
        "ma20": tech["latest"]["MA20"],
        "mom20": round(float(mom20), 2) if mom20 is not None else None,
        "vol_ratio": tech["latest"]["vol_ratio"],
        "range_pos": round(float(rng), 3),
        "trend": f["trend_score"], "momentum": f["momentum_score"], "volatility": f["volatility_score"],
        "score": round(f["trend_score"] * 0.4 + f["momentum_score"] * 0.4 + f["volatility_score"] * 0.2, 1),
    }
    _save_code_cache(code, {"schema": _CACHE_SCHEMA, "date": today,
                           "updated_at": cache_generated_at, "data": fv})
    return fv


def _match(fv, r):
    if r.get("rsi_max") is not None and (fv["rsi"] is None or fv["rsi"] > r["rsi_max"]):
        return False
    if r.get("rsi_min") is not None and (fv["rsi"] is None or fv["rsi"] < r["rsi_min"]):
        return False
    if r.get("ma20") == "above" and (fv["close"] is None or fv["ma20"] is None or fv["close"] <= fv["ma20"]):
        return False
    if r.get("ma20") == "below" and (fv["close"] is None or fv["ma20"] is None or fv["close"] >= fv["ma20"]):
        return False
    if r.get("mom20_min") is not None and (fv["mom20"] is None or fv["mom20"] < r["mom20_min"]):
        return False
    if r.get("mom20_max") is not None and (fv["mom20"] is None or fv["mom20"] > r["mom20_max"]):
        return False
    if r.get("vol_ratio_min") is not None and (fv["vol_ratio"] is None or fv["vol_ratio"] < r["vol_ratio_min"]):
        return False
    if r.get("range_pos_max") is not None and (fv["range_pos"] is None or fv["range_pos"] > r["range_pos_max"]):
        return False
    return True


def _is_risk_name(name):
    value = str(name or "").upper().replace(" ", "")
    return value.startswith("*ST") or value.startswith("ST") or value.startswith("退") or value.endswith("退")


def all_codes_info(local_only=False):
    meta = {
        "scope": "沪深京 A 股代码名称表",
        "provider": "AKShare stock_info_a_code_name",
        "source_count": None,
        "eligible_count": None,
        "risk_excluded_count": None,
        "invalid_code_count": None,
        "local_cache_updated_at": None,
        "local_cache_status": None,
    }
    try:
        if local_only:
            names, map_as_of, fresh = sa._name_map_local()
            meta["local_cache_status"] = "fresh" if fresh else ("stale" if names else "missing")
        else:
            names = sa._name_map() or {}
            map_as_of = getattr(sa, "_NAME_MAP_AS_OF", None)
            fresh = True
        if not isinstance(names, dict):
            raise ValueError("股票名称映射不是字典")
        if isinstance(map_as_of, (int, float)):
            meta["local_cache_updated_at"] = datetime.fromtimestamp(
                map_as_of
            ).astimezone().isoformat(timespec="minutes")
        valid = [(str(code), name) for code, name in names.items()
                 if str(code).isdigit() and len(str(code)) == 6]
        eligible = [code for code, name in valid if not _is_risk_name(name)]
        meta.update({"source_count": len(names), "eligible_count": len(set(eligible)),
                     "risk_excluded_count": len(valid) - len(eligible),
                     "invalid_code_count": len(names) - len(valid)})
        if not eligible:
            raise ValueError("股票名称映射为空或没有可用代码")
        warning = ("股票清单本地缓存已过期；下列候选仅供查看，覆盖分母未知，请更新全市场清单"
                   if local_only and not fresh else None)
        if warning:
            meta["error"] = warning
        return sorted(set(eligible)), warning, meta
    except Exception as e:
        if "names" in locals() and isinstance(names, dict):
            if meta["source_count"] is None:
                valid_pairs = [(str(code), name) for code, name in names.items()
                               if str(code).isdigit() and len(str(code)) == 6]
                meta.update({"source_count": len(names),
                             "eligible_count": sum(1 for _, name in valid_pairs if not _is_risk_name(name)),
                             "risk_excluded_count": sum(1 for _, name in valid_pairs if _is_risk_name(name)),
                             "invalid_code_count": len(names) - len(valid_pairs)})
        return [], f"全市场股票清单不可用：{str(e)[:160]}", meta


def all_codes():
    return all_codes_info()[0]


def _scan_one(code, refresh):
    error = None
    details = {}
    _FACTOR_DIAGNOSTICS.value = None
    try:
        value = _factors(code, refresh=refresh)
        if value is None:
            diagnostic = getattr(_FACTOR_DIAGNOSTICS, "value", None)
            if isinstance(diagnostic, dict):
                error = diagnostic["error"]
                details = {k: diagnostic[k] for k in ("kind", "available_rows", "required_rows", "invalid_rows", "duplicate_rows") if k in diagnostic}
            else:
                error = "行情无有效数据或有效历史不足25根K线"
        elif not value.get("as_of"):
            error = "行情缺少有效的最近交易日期"
    except Exception as e:
        error = f"{type(e).__name__}: {str(e)[:160]}"
    with _SCAN_LOCK:
        _SCAN["done"] += 1
        if error:
            _SCAN["failed"] += 1
            _SCAN["failed_by_code"][code] = {
                "error": error,
                "at": market_clock.now().isoformat(timespec="seconds"),
                **details,
            }
        else:
            _SCAN["succeeded"] += 1
            _SCAN["failed_by_code"].pop(code, None)
        should_persist = _SCAN["done"] % 50 == 0 or (_SCAN["failed"] > 0 and _SCAN["failed"] % 10 == 0)
    if should_persist:
        _persist_scan_state()


def start_scan(codes=None, workers=16, refresh=False):
    with _SCAN_START_LOCK:
        with _SCAN_LOCK:
            if _SCAN["running"]:
                return False
            today = market_clock.now().strftime("%Y-%m-%d")
            previous_failures = (dict(_SCAN.get("failed_by_code", {}))
                                 if _SCAN.get("date") == today else {})
            _SCAN.update({"date": today, "running": True, "stage": "preparing",
                          "total": 0, "done": 0, "succeeded": 0, "failed": 0,
                          "started_at": market_clock.now().isoformat(timespec="seconds"),
                          "finished_at": None, "error": None,
                          "message": "正在后台准备股票清单与有效缓存",
                          "interrupted": False, "failed_by_code": {}, "state_error": None,
                          "prepare_ms": None, "execute_ms": None, "total_ms": None,
                          "worker_count": workers})
        _persist_scan_state()
        try:
            threading.Thread(target=_run_scan,
                             args=(codes, workers, refresh, previous_failures),
                             daemon=True, name="market-scan").start()
        except Exception as error:
            with _SCAN_LOCK:
                _SCAN.update({"running": False, "stage": "failed",
                              "error": f"扫描线程未启动：{type(error).__name__}",
                              "message": "无法启动扫描任务",
                              "finished_at": market_clock.now().isoformat(timespec="seconds")})
            _persist_scan_state()
            return False
        return True


def _run_scan(codes, workers, refresh, previous_failures):
    started = time.monotonic()
    try:
        _scan_work(codes, workers, refresh, previous_failures, started)
    except Exception as error:
        with _SCAN_LOCK:
            _SCAN["error"] = f"扫描准备或执行失败：{type(error).__name__}: {str(error)[:160]}"
    finally:
        with _SCAN_LOCK:
            _SCAN["total_ms"] = round((time.monotonic() - started) * 1000, 1)
            if _SCAN["prepare_ms"] is None:
                _SCAN["prepare_ms"] = _SCAN["total_ms"]
            _SCAN["running"] = False
            _SCAN["finished_at"] = market_clock.now().isoformat(timespec="seconds")
            if _SCAN["error"]:
                _SCAN["stage"] = "failed"
                _SCAN["message"] = _SCAN["error"]
            else:
                _SCAN["stage"] = "done"
                if _SCAN["message"] != "当日有效缓存已完整，无需重复扫描":
                    _SCAN["message"] = ("扫描结束，仍有股票失败" if _SCAN["failed"]
                                        else "扫描完成")
        _persist_scan_state()


def _scan_work(codes, workers, refresh, previous_failures, started=None):
    started = time.monotonic() if started is None else started
    codes_error = None
    if codes is None:
        codes, codes_error, _ = all_codes_info()
    codes = list(dict.fromkeys(str(code).strip() for code in codes if str(code).strip()))
    if not codes:
        message = codes_error or "扫描股票清单为空，未启动扫描"
        with _SCAN_LOCK:
            _SCAN.update({"error": message, "message": message, "stage": "failed"})
        return
    today = market_clock.now().strftime("%Y-%m-%d")
    current_codes = set()
    if not refresh:
        for code in codes:
            if _cache_is_current(_load_code_cache(code), today):
                current_codes.add(code)
    pending = [code for code in codes if refresh or code not in current_codes]
    previous_failures = {code: value for code, value in previous_failures.items()
                         if code in pending and isinstance(value, dict)}
    with _SCAN_LOCK:
        _SCAN.update({"date": today, "running": True, "stage": "scanning",
                      "prepare_ms": round((time.monotonic() - started) * 1000, 1),
                      "total": len(codes),
                      "done": len(current_codes), "succeeded": len(current_codes), "failed": 0,
                      "finished_at": None if pending else market_clock.now().isoformat(timespec="seconds"),
                      "error": None, "message": "正在补齐未缓存股票" if pending else "当日有效缓存已完整，无需重复扫描",
                      "interrupted": False, "failed_by_code": previous_failures, "state_error": None})
    _persist_scan_state()
    if not pending:
        with _SCAN_LOCK:
            _SCAN["execute_ms"] = 0.0
        return
    executing = time.monotonic()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            list(executor.map(lambda code: _scan_one(code, refresh), pending))
    finally:
        with _SCAN_LOCK:
            _SCAN["execute_ms"] = round((time.monotonic() - executing) * 1000, 1)


def scan_status():
    with _SCAN_LOCK:
        status = {key: value for key, value in _SCAN.items() if key != "failed_by_code"}
        failed = _SCAN.get("failed_by_code", {})
        status["failed_codes_count"] = len(failed)
        status["failed_examples"] = [
            {"code": code, **failed[code]} for code in sorted(failed)[:10]
        ]
        return status


def _cached_today():
    today = market_clock.now().strftime("%Y-%m-%d")
    now = market_clock.now().timestamp()
    with _TODAY_ROWS_LOCK:
        if _TODAY_ROWS["date"] == today and now - _TODAY_ROWS["at"] < 15:
            return [dict(row) for row in _TODAY_ROWS["rows"]]
    out = []
    for fn in os.listdir(_CACHE_DIR):
        if not fn.endswith(".json"):
            continue
        try:
            with open(os.path.join(_CACHE_DIR, fn), encoding="utf-8") as f:
                v = json.load(f)
        except Exception:
            continue
        if isinstance(v, dict) and v.get("date") == today and isinstance(v.get("data"), dict):
            row = dict(v["data"])
            as_of = _normalize_date(row.get("as_of"))
            if not as_of or as_of > today:
                row["date_unknown"] = True
            else:
                row["as_of"] = as_of
                age = (date.fromisoformat(today) - date.fromisoformat(as_of)).days
                row["quote_age_days"] = age
                row["quote_state"] = ("same_day_unconfirmed" if age == 0 else
                                      "stale" if age > MAX_RANK_QUOTE_AGE_DAYS else "prior_day")
            if (v.get("schema") != _CACHE_SCHEMA or not isinstance(row.get("data_quality"), list)
                    or "data_source" not in row or not row.get("cache_generated_at")
                    or row.get("volume_unit") not in VOLUME_LABELS.values()):
                row["quality_unknown"] = True
            out.append(row)
    with _TODAY_ROWS_LOCK:
        _TODAY_ROWS.update({"at": market_clock.now().timestamp(), "date": today, "rows": [dict(row) for row in out]})
    return out


def cached_candidates(limit=10):
    """Return the highest-scoring factors cached today, with explicit coverage."""
    names, _, _ = sa._name_map_local()
    codes, codes_error, universe_metadata = all_codes_info(local_only=True)
    allowed = set(codes)
    entries = [row for row in _cached_today() if row.get("code") in allowed]
    rows = [row for row in entries if not row.get("date_unknown") and not row.get("quality_unknown")]
    rows.sort(key=lambda x: x.get("score", 0), reverse=True)
    universe_count = len(codes) if not codes_error else None
    cached_count = len(rows)
    scan = scan_status()
    full_coverage = cached_count >= universe_count if universe_count else False
    scan_complete = bool(universe_count and scan.get("total", 0) >= universe_count
                         and scan.get("done", 0) >= scan.get("total", 1))
    top = []
    for i, item in enumerate(rows[:max(0, int(limit))], 1):
        row = dict(item)
        row["rank"] = i
        row["name"] = names.get(row.get("code"), row.get("code"))
        top.append(row)
    date_fields = _cache_date_summary(entries)
    return {
        "date": market_clock.now().strftime("%Y-%m-%d"),
        **date_fields,
        "cached_count": cached_count,
        "cache_entries_count": len(entries),
        "pe_cached_count": sum(1 for row in rows if row.get("pe_percentile") is not None),
        "universe_count": universe_count,
        "coverage_pct": round(cached_count / universe_count * 100, 2) if universe_count else None,
        "full_coverage": full_coverage,
        "scan_complete": scan_complete,
        "universe_error": codes_error,
        "universe_meta": universe_metadata,
        "scan": scan,
        "rows": top,
    }


def screen(rules=None, universe="sample", refresh_missing=True, progress=None):
    rules = rules or {}
    if universe == "full":
        codes, codes_error, _ = all_codes_info(local_only=True)
        allowed = set(codes) if not codes_error else set()
        out = [row for row in _cached_today()
               if row.get("code") in allowed and not row.get("date_unknown")
               and not row.get("quality_unknown")]
    elif refresh_missing:
        out = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(_factors, code): code for code in UNIVERSE}
            for done, future in enumerate(concurrent.futures.as_completed(futures), 1):
                fv = future.result()
                if fv is not None:
                    out.append(fv)
                if progress:
                    progress(done, len(UNIVERSE))
    else:
        allowed = set(UNIVERSE)
        out = [fv for fv in _cached_today() if fv.get("code") in allowed
               and not fv.get("date_unknown") and not fv.get("quality_unknown")]
    names, _, _ = sa._name_map_local()
    out = [fv for fv in out if not _is_risk_name(names.get(fv.get("code"), ""))]
    out = [fv for fv in out if _match(fv, rules)]
    if rules.get("pe_max") is not None and out:
        kept = []
        for fv in out:
            pe = fv.get("pe_percentile")
            if pe is None and refresh_missing:
                try:
                    pe = sa._rank_fundamentals(fv["code"]).get("pe_percentile")
                    fv["pe_percentile"] = pe
                except Exception:
                    pe = None
            if pe is not None and pe <= rules["pe_max"]:
                kept.append(fv)
        out = kept
    out.sort(key=lambda x: x["score"], reverse=True)
    for i, fv in enumerate(out):
        fv["rank"] = i + 1
        fv["name"] = names.get(fv["code"]) or fv["code"]
    return out


def cache_coverage(universe="full"):
    all_rows = _cached_today()
    if universe == "full":
        codes, codes_error, universe_metadata = all_codes_info(local_only=True)
    else:
        codes, codes_error = UNIVERSE, None
        universe_metadata = {"scope": "固定核心观察池", "provider": "screener.UNIVERSE",
                             "source_count": len(UNIVERSE), "eligible_count": len(UNIVERSE),
                             "risk_excluded_count": 0, "invalid_code_count": 0,
                             "local_cache_updated_at": None}
    allowed = set(codes)
    entries = [r for r in all_rows if r.get("code") in allowed]
    rows = [r for r in entries if not r.get("date_unknown") and not r.get("quality_unknown")]
    total = len(allowed) if not codes_error else None
    count = len(rows)
    return {"date": market_clock.now().strftime("%Y-%m-%d"), **_cache_date_summary(entries), "cached_count": count,
            "cache_entries_count": len(entries),
            "universe_count": total,
            "universe_error": codes_error,
            "universe_meta": universe_metadata,
            "coverage_pct": round(count / total * 100, 2) if total else None,
            "pe_cached_count": sum(1 for r in rows if r.get("pe_percentile") is not None),
            "scan": scan_status()}


def _cache_date_summary(rows):
    today = market_clock.now().strftime("%Y-%m-%d")
    normalized = [(row, _normalize_date(row.get("as_of"))) for row in rows]
    dated_rows = [(row, as_of) for row, as_of in normalized if as_of and as_of <= today]
    dates = sorted({as_of for _, as_of in dated_rows})
    latest = dates[-1] if dates else None
    return {
        "as_of_latest": latest,
        "as_of_oldest": dates[0] if dates else None,
        "older_than_latest_count": sum(1 for _, as_of in dated_rows if as_of < latest) if latest else 0,
        "same_day_unconfirmed_count": sum(1 for _, as_of in dated_rows if as_of == today),
        "stale_quote_count": sum(1 for _, as_of in dated_rows
                                 if (date.fromisoformat(today) - date.fromisoformat(as_of)).days > MAX_RANK_QUOTE_AGE_DAYS),
        "stale_quote_age_days": MAX_RANK_QUOTE_AGE_DAYS,
        "unknown_date_count": sum(1 for _, as_of in normalized if not as_of or as_of > today),
        "quality_unknown_count": sum(1 for row in rows if row.get("quality_unknown")),
        "source_retrieved_at_latest": max((str(row.get("source_retrieved_at")) for row, _ in dated_rows
                                            if row.get("source_retrieved_at")), default=None),
        "cache_generated_at_latest": max((str(row.get("cache_generated_at")) for row, _ in dated_rows
                                          if row.get("cache_generated_at")), default=None),
    }
