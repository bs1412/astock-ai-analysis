"""Bounded public paired raw/qfq bars and dividend/rights source records."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import hashlib
import json
import math
import time
from zoneinfo import ZoneInfo

import pandas as pd
import stock_agent as sa
import public_daily_cache

FIXED_CODES = ("600000", "000001", "300750", "688981")
LOOKBACK_DAYS = 1460


def _day(value):
    parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else parsed.date().isoformat()


def _number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError, OverflowError):
        return None


def frame_payload(frame):
    columns = ["date", "open", "high", "low", "close", "volume", "amount"]
    rows = [[_day(row.get("date")), *[_number(row.get(key)) for key in columns[1:]]]
            for _,row in frame.iterrows()]
    return {"columns": columns, "rows": rows, "attrs": dict(frame.attrs)}


def frame_from_payload(payload):
    frame = pd.DataFrame(payload["rows"], columns=payload["columns"])
    frame["date"] = pd.to_datetime(frame["date"])
    frame.attrs.update(payload.get("attrs", {}))
    return frame


def content_id(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _bars(code, start, end, adjustment):
    frame = sa._tencent_daily_frame(sa._prefix(code)+code, start, end, timeout=6, adjustment=adjustment)
    if frame is None or frame.empty:
        raise ValueError("public daily bars unavailable")
    expected = "qfqday" if adjustment else "day"
    fields = frame.attrs.get("provider_price_fields", [])
    if (not fields or (not adjustment and fields != [expected])
            or (adjustment and not set(fields).issubset({"qfqday", "day"}))):
        raise ValueError("requested and returned adjustment fields differ")
    latest = frame.iloc[-1]
    ratio = float(latest["amount"])/(float(latest["volume"])*float(latest["close"]))
    if 25 <= ratio <= 400 and code.startswith("000"):
        frame["volume"] *= 100
    elif not .25 <= ratio <= 4:
        raise ValueError("public volume unit evidence ambiguous")
    at = datetime.now().astimezone().isoformat(timespec="seconds")
    basis = "qfq" if fields == ["qfqday"] else "qfq_request_raw_field" if adjustment else "none"
    frame.attrs.update({"source": "tencent", "interval": "1d", "adjustment": basis,
                        "volume_unit": "shares", "source_retrieved_at": at, "cache_generated_at": at})
    return frame


def _actions(code, start_day, end_day):
    result, sources, errors = [], [], []
    for indicator in ("分红", "配股"):
        started = time.perf_counter()
        try:
            with sa._default_http_timeout(6):
                frame = sa.ak.stock_history_dividend_detail(symbol=code, indicator=indicator)
            records = []
            for _,row in frame.iterrows():
                announcement = _day(row.get("公告日期"))
                ex_day = _day(row.get("除权除息日") if indicator == "分红" else row.get("除权日"))
                if ex_day and start_day <= ex_day <= end_day:
                    if indicator == "分红" and str(row.get("进度", "")).strip() != "实施":
                        continue
                    dividend = [_number(row.get(key)) for key in ("派息", "送股", "转增")] if indicator == "分红" else [0,0,0]
                    valid_numbers = all(value is not None and value >= 0 for value in dividend)
                    record = {"code": code, "announcement_date": announcement, "ex_date": ex_day,
                              "record_date": _day(row.get("股权登记日")),
                              "cash_per_share": round(dividend[0]/10, 8) if valid_numbers else None,
                              "bonus_ratio": round((dividend[1]+dividend[2])/10, 8) if valid_numbers else None,
                              "shares_available_date": _day(row.get("红股上市日")),
                              "cash_available_date": None, "kind": "dividend_bonus" if indicator == "分红" and valid_numbers else "unresolved_numbers" if indicator == "分红" else "rights_unmodeled",
                              "units_contract": "sina_table_per10_shares_gross_cash",
                              "source": "sina_via_akshare", "source_coverage": "provider_table_not_official_complete",
                              "source_row": {str(key): str(value) for key,value in row.items()}}
                    result.append(record)
                    records.append(record)
            sources.append({"indicator": indicator, "status": "ok" if not frame.empty else "empty",
                            "source_rows": len(frame), "in_range_rows": len(records),
                            "source_url": "https://vip.stock.finance.sina.com.cn/corp/go.php/vISSUE_ShareBonus/stockid/"+code+".phtml",
                            "elapsed_ms": round((time.perf_counter()-started)*1000,1)})
        except Exception as error:
            errors.append({"indicator": indicator, "error_type": type(error).__name__})
            sources.append({"indicator": indicator, "status": "failed",
                            "elapsed_ms": round((time.perf_counter()-started)*1000,1)})
    result.sort(key=lambda event:(event["ex_date"],event["kind"]))
    return result, sources, errors


def fetch_one(code, today):
    started = time.perf_counter()
    first = today-timedelta(days=LOOKBACK_DAYS)
    item = {"code": code, "errors": [], "raw": None, "signal": None, "events": [],
            "event_sources": [], "event_coverage": "unavailable"}
    for key, adjust in (("raw", ""), ("signal", "qfq")):
        try:
            first_text,end_text=first.strftime("%Y%m%d"),today.strftime("%Y%m%d")
            frame = public_daily_cache.daily(code,first_text,end_text,
                lambda:_bars(code,first_text,end_text,adjust),
                namespace="research_qfq_tencent_v219" if adjust else "research_raw_tencent_v219")
            if frame is None or frame.empty:
                raise ValueError("public research bars unavailable")
            frame = frame.loc[frame["date"].dt.date.lt(today)].reset_index(drop=True)
            item[key] = frame_payload(frame)
        except Exception as error:
            item["errors"].append({"part": key, "error_type": type(error).__name__})
    if item["raw"] and item["signal"] and item["signal"]["attrs"]["adjustment"] != "qfq":
        if item["raw"]["rows"] == item["signal"]["rows"]:
            item["signal"]["attrs"]["adjustment"] = "qfq_request_raw_identical_observed"
            item["signal_basis_note"] = "前复权请求返回day字段，与同源未复权逐值相同；无调整仅为观测结果，事件完整性未独立证明"
        else:
            item["signal"] = None
            item["errors"].append({"part": "signal", "error_type": "ambiguous_adjustment_fields"})
    item["events"], item["event_sources"], failures = _actions(code, first.isoformat(), (today-timedelta(days=1)).isoformat())
    item["errors"].extend({"part": "events", **error} for error in failures)
    item["event_coverage"] = "failed_part" if failures else "provider_only"
    item["retrieved_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
    item["elapsed_ms"] = round((time.perf_counter()-started)*1000,1)
    item["input_id"] = content_id(item)
    return item


def fetch_inputs(today=None, progress=None):
    today = today or datetime.now(ZoneInfo("Asia/Shanghai")).date()
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {code:executor.submit(fetch_one, code, today) for code in FIXED_CODES}
        rows = []
        for code in FIXED_CODES:
            rows.append(futures[code].result())
            if progress:
                progress("inputs", len(rows), len(FIXED_CODES), "已核对公开历史输入 "+code)
    return {"schema": "paired_public_history_v1", "evaluation_date": today.isoformat(),
            "fixed_codes": list(FIXED_CODES), "inputs": rows,
            "retrieved_at": datetime.now().astimezone().isoformat(timespec="seconds")}
