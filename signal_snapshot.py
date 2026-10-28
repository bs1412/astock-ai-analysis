# -*- coding: utf-8 -*-
"""Shared completed-daily-bar qualification for current rule outputs.

Provider completion and real execution remain unverified even for eligible bars.
"""
import math
import hashlib
import json
import rule_contract
import market_clock
from datetime import datetime, time
from zoneinfo import ZoneInfo

import pandas as pd


PRICE_COLUMNS = ("open", "high", "low", "close", "volume")
SOURCE_NAMES = ("tencent", "sina", "eastmoney")
SHANGHAI = ZoneInfo("Asia/Shanghai")
# Engineering review thresholds, not measured provider error bounds.
CLOSE_REVIEW_FRACTION = 0.005
VOLUME_REVIEW_FRACTION = 0.05


def clean_frame(frame, today):
    if frame is None or frame.empty:
        return None, 0
    df = frame.copy()
    df["date"] = pd.to_datetime(df.get("date"), errors="coerce")
    for column in PRICE_COLUMNS:
        df[column] = pd.to_numeric(df.get(column), errors="coerce")
    valid_close = df["close"].map(lambda value: pd.notna(value) and math.isfinite(float(value))
                                    and float(value) > 0)
    rejected = int((df["date"].isna() | ~valid_close).sum())
    df = (df.loc[df["date"].notna() & valid_close & df["date"].dt.date.lt(today)]
          .drop_duplicates("date", keep="last").sort_values("date").reset_index(drop=True))
    df.attrs.update(frame.attrs)
    return (df if not df.empty else None), rejected


def _latest_completed_raw(frame, today):
    if frame is None or frame.empty or "date" not in frame:
        return None, None
    dated = frame.copy()
    dated["date"] = pd.to_datetime(dated["date"], errors="coerce")
    dated = (dated.loc[dated["date"].notna() & dated["date"].dt.date.lt(today)]
             .drop_duplicates("date", keep="last").sort_values("date"))
    if dated.empty:
        return None, None
    row = dated.iloc[-1]
    return row, row["date"].date()


def latest_bar_quality(row):
    if row is None:
        return ["没有可用的已结束日线"]
    values = {}
    issues = []
    for column in PRICE_COLUMNS:
        value = pd.to_numeric(pd.Series([row.get(column)]), errors="coerce").iloc[0]
        if pd.isna(value) or not math.isfinite(float(value)):
            issues.append(f"最新日线{column}缺失或无效")
        else:
            values[column] = float(value)
    if len(values) == len(PRICE_COLUMNS):
        if min(values[key] for key in ("open", "high", "low", "close")) <= 0:
            issues.append("最新日线价格非正数")
        if values["volume"] <= 0:
            issues.append("最新日线成交量为零或负数；停牌或数据缺失原因未核实")
        if (values["high"] < max(values["open"], values["close"])
                or values["low"] > min(values["open"], values["close"])
                or values["high"] < values["low"]):
            issues.append("最新日线高低价与开收盘关系异常")
    return issues


def quote_snapshot_id(code, frame, today):
    """Identify the latest completed quote and its local retrieval context."""
    row, trade_date = _latest_completed_raw(frame, today)
    if row is None:
        return None
    payload = {"code": str(code), "trade_date": trade_date.isoformat(),
               "source": frame.attrs.get("source"),
               "interval": frame.attrs.get("interval"),
               "adjustment": frame.attrs.get("adjustment"),
               "volume_unit": frame.attrs.get("volume_unit"),
               "retrieved_at": frame.attrs.get("source_retrieved_at"),
               "cache_generated_at": frame.attrs.get("cache_generated_at"),
               "bar": {key: str(row.get(key)) for key in PRICE_COLUMNS}}
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def compare_sources(primary, secondary, today, code=None):
    """Compare two provided, normalized daily frames; never fetch a source."""
    result = {"status": "unavailable", "sources": [], "trade_dates": [], "reasons": [],
              "close_diff_pct": None, "volume_diff_pct": None,
              "secondary_source_retrieved_at": secondary.attrs.get("source_retrieved_at") if secondary is not None else None,
              "secondary_cache_state": secondary.attrs.get("request_cache_state") if secondary is not None else None,
              "primary_snapshot_id": quote_snapshot_id(code, primary, today) if code is not None else None}
    if primary is None or primary.empty or secondary is None or secondary.empty:
        result["reasons"] = ["缺少第二份可比较的日线"]
        return result
    result["sources"] = [primary.attrs.get("source"), secondary.attrs.get("source")]
    contracts = ("interval", "adjustment", "volume_unit")
    for frame in (primary, secondary):
        if frame.attrs.get("source") not in SOURCE_NAMES:
            result["reasons"].append("行情来源不在已登记列表")
        for key in contracts:
            if frame.attrs.get(key) is None:
                result["reasons"].append(f"{key}口径未知")
    if result["sources"][0] == result["sources"][1]:
        result["reasons"].append("两份日线来自同一行情源")
    for key in contracts:
        if primary.attrs.get(key) != secondary.attrs.get(key):
            result["reasons"].append(f"两源{key}口径不同")
    if result["reasons"]:
        result["status"] = "incomparable"
        return result
    first, first_date = _latest_completed_raw(primary, today)
    second, second_date = _latest_completed_raw(secondary, today)
    result["trade_dates"] = [first_date.isoformat() if first_date else None,
                             second_date.isoformat() if second_date else None]
    if first is None or second is None:
        result["status"] = "incomparable"
        result["reasons"] = ["至少一个来源没有已结束日线"]
        return result
    if first_date != second_date:
        result["status"] = "conflict"
        result["reasons"] = ["两源最新交易日期不同"]
        return result
    issues = latest_bar_quality(first) + latest_bar_quality(second)
    if issues:
        result["status"] = "incomparable"
        result["reasons"] = issues
        return result
    for field, threshold in (("close", CLOSE_REVIEW_FRACTION),
                             ("volume", VOLUME_REVIEW_FRACTION)):
        left, right = float(first[field]), float(second[field])
        difference = abs(left - right) / max(abs(left), abs(right))
        result[f"{field}_diff_pct"] = round(difference * 100, 3)
        if difference > threshold:
            result["reasons"].append(f"两源最新{field}差异超过预登记工程复核阈值")
    result["status"] = "conflict" if result["reasons"] else "consistent"
    return result


def _parse_time(value, label, issues):
    if not value:
        issues.append(f"{label}未知")
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        issues.append(f"{label}无法解析")
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        issues.append(f"{label}缺少时区")
        return None
    return parsed.astimezone(SHANGHAI)


def data_snapshot(code, raw, clean, rejected, today, max_age_days=7, now=None):
    row, trade_date = _latest_completed_raw(raw, today)
    snapshot_id = quote_snapshot_id(code, raw, today)
    source = raw.attrs.get("source") if raw is not None else None
    retrieved_at = raw.attrs.get("source_retrieved_at") if raw is not None else None
    cache_generated_at = raw.attrs.get("cache_generated_at") if raw is not None else None
    blocking = latest_bar_quality(row)
    daily_time=market_clock.daily_evidence(trade_date)
    if trade_date and market_clock.day_status(trade_date)['is_trading_day'] is not True:
        blocking.append('最新日线源日期休市或交易日历未核实，不能登记有效信号')
    if source not in SOURCE_NAMES:
        blocking.append("行情来源未知或未登记")
    for key, expected, label in (("interval", "1d", "周期"),
                                 ("adjustment", "qfq", "复权方式"),
                                 ("volume_unit", "shares", "成交量单位")):
        if raw is None or raw.attrs.get(key) != expected:
            blocking.append(f"{label}未知或不符合日线前复权股数口径")
    retrieved = _parse_time(retrieved_at, "行情取回时间", blocking)
    cached = _parse_time(cache_generated_at, "缓存生成时间", blocking)
    now = now or datetime.now(SHANGHAI)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must have a timezone")
    now = now.astimezone(SHANGHAI)
    if trade_date and retrieved and retrieved < datetime.combine(trade_date, time.min, SHANGHAI):
        blocking.append("行情取回时间早于源交易日")
    if retrieved and cached and cached < retrieved:
        blocking.append("缓存生成时间早于行情取回时间")
    if retrieved and retrieved > now:
        blocking.append("行情取回时间晚于本机当前时间")
    if cached and cached > now:
        blocking.append("缓存生成时间晚于本机当前时间")
    comparison = raw.attrs.get("source_comparison") if raw is not None else None
    comparison_status = comparison.get("status") if isinstance(comparison, dict) else "unchecked"
    comparison_reasons = comparison.get("reasons", []) if isinstance(comparison, dict) else []
    if comparison_status not in ("unchecked", "unavailable", "consistent", "conflict", "incomparable"):
        comparison_status = "unchecked"
        comparison_reasons = []
    if comparison_status == "unavailable" and isinstance(comparison, dict):
        recorded_id = comparison.get("primary_snapshot_id")
        if recorded_id != snapshot_id:
            comparison_status = "unchecked"
            comparison_reasons = ["第二来源失败记录与当前报价快照不一致，需重新核对"]
    if comparison_status in ("consistent", "conflict", "incomparable"):
        if (not snapshot_id or not isinstance(comparison, dict)
                or comparison.get("primary_snapshot_id") != snapshot_id):
            comparison_status = "incomparable"
            comparison_reasons = ["跨源核对记录与当前报价快照不一致"]
    if comparison_status in ("consistent", "conflict", "incomparable"):
        sources = comparison.get("sources", [])
        dates = comparison.get("trade_dates", [])
        if ((sources and (len(sources) != 2 or sources[0] != source))
                or (dates and (len(dates) != 2 or trade_date is None
                               or dates[0] != trade_date.isoformat()))):
            comparison_status = "incomparable"
            comparison_reasons = ["跨源核对记录与当前主来源或交易日不一致"]
    if comparison_status == "consistent":
        sources = comparison.get("sources", [])
        dates = comparison.get("trade_dates", [])
        if (len(sources) != 2 or sources[1] == source or len(dates) != 2
                or trade_date is None or dates != [trade_date.isoformat()] * 2):
            comparison_status = "incomparable"
            comparison_reasons = ["跨源一致记录缺少两份不同来源的同日证据"]
    if comparison_status in ("conflict", "incomparable"):
        blocking.extend(f"跨源核对：{reason}" for reason in comparison_reasons)
        if not comparison_reasons:
            blocking.append("跨源核对发现冲突或无法比较")
    issues = list(blocking)
    if rejected:
        issues.append(f"历史区间剔除无日期或无效收盘日线 {rejected} 条")
    if comparison_status in ("unchecked", "unavailable"):
        issues.append("跨源行情尚未完成核对")
        if comparison_reasons:
            issues.extend(f"第二来源：{reason}" for reason in comparison_reasons)
    age_days = (today - trade_date).days if trade_date else None
    stale = age_days is not None and age_days > max_age_days
    eligible = bool(clean is not None and trade_date is not None and not stale and not blocking
                    and clean["date"].iloc[-1].date() == trade_date)
    if trade_date is None:
        status = "missing"
    elif stale:
        status = "stale"
    elif not eligible:
        status = "quality_warning"
    elif rejected:
        status = "history_warning"
    else:
        status = "available"
    code = str(code)
    market = ("SH" if code.startswith("6") else "SZ" if code.startswith(("0", "3"))
              else "BJ" if code.startswith(("4", "8", "920")) else "unknown")
    return {"code": code, "market": market, "interval": "1d", "adjustment": "前复权",
            "volume_unit": ({"shares":"股", "depositary_receipts":"份（存托凭证）"}.get(raw.attrs.get("volume_unit"), "未知") if raw is not None else "未知"),
            "source": source, "source_trade_date": trade_date.isoformat() if trade_date else None,
            "quote_snapshot_id": snapshot_id,
            "market_time":market_clock.snapshot(now), "price_evidence":daily_time,
            "retrieved_at": retrieved_at, "cache_generated_at": cache_generated_at,
            "is_completed_session": trade_date is not None,
            "provider_completeness_confirmed": False,
            "age_calendar_days": age_days, "status": status,
            "current_signal_eligible": eligible,
            "cross_source_status": comparison_status,
            "cross_source_reasons": comparison_reasons,
            "cross_source_sources": (comparison.get("sources", []) if isinstance(comparison, dict)
                                     and comparison_status in ("consistent", "conflict")
                                     else []),
            "cross_source_attempted_sources": (comparison.get("attempted_sources", [])
                                               if isinstance(comparison, dict)
                                               and comparison_status != "unchecked" else []),
            "cross_source_fallback_failures": (comparison.get("fallback_failures", [])
                                              if isinstance(comparison, dict)
                                              and comparison_status != "unchecked" else []),
            "cross_source_checked_at": comparison.get("checked_at") if isinstance(comparison, dict) else None,
            "cross_source_elapsed_ms": comparison.get("elapsed_ms") if isinstance(comparison, dict) else None,
            "cross_source_secondary_retrieved_at": comparison.get("secondary_source_retrieved_at") if isinstance(comparison, dict) else None,
            "cross_source_secondary_cache_state": comparison.get("secondary_cache_state") if isinstance(comparison, dict) else None,
            "cross_source_cache_hit": comparison.get("cache_hit") if isinstance(comparison, dict) else None,
            "cross_source_provider_invocations": (comparison.get("provider_invocations")
                                                    if isinstance(comparison, dict) else None),
            "blocking_issues": blocking, "quality_issues": issues}


def unavailable_reason(snapshot, max_age_days=7):
    if snapshot["status"] == "stale":
        details = "；".join(snapshot["blocking_issues"])
        return f"最新完整日线超过{max_age_days}个自然日" + (f"；{details}" if details else "")
    return "；".join(snapshot["blocking_issues"]) or "行情不足，无法核对当前规则"


def require_independent_source(snapshot):
    """Qualify an active signal or assumed fill only after independent agreement."""
    if snapshot.get("current_signal_eligible") and snapshot.get("cross_source_status") != "consistent":
        reason = "独立第二来源尚未确认一致，暂不登记有效信号或假定成交"
        snapshot.setdefault("blocking_issues", []).append(reason)
        snapshot.setdefault("quality_issues", []).append(reason)
        snapshot["current_signal_eligible"] = False
        snapshot["status"] = "quality_warning"
    return snapshot


def require_rule_dates(snapshot, *active_rules):
    """Qualify active rules against the quote day and independent source evidence."""
    if not snapshot.get("current_signal_eligible"):
        return snapshot
    for rule_state, active_status in active_rules:
        if (rule_state is not None and rule_state.get("status") == active_status
                and rule_state.get("as_of") != snapshot.get("source_trade_date")):
            reason = "规则信号日与当前报价快照交易日不一致"
            snapshot["blocking_issues"].append(reason)
            snapshot["quality_issues"].append(reason)
            snapshot["current_signal_eligible"] = False
            snapshot["status"] = "quality_warning"
            break
        if rule_state is not None and rule_state.get("status") == active_status:
            require_independent_source(snapshot)
    return snapshot


def signal_trace(rule_state, snapshot, active_status):
    """Record signal timing without inventing the next exchange session date."""
    active = (bool(snapshot.get("current_signal_eligible"))
              and snapshot.get("cross_source_status") == "consistent"
              and rule_state is not None and rule_state.get("status") == active_status
              and rule_state.get("as_of") == snapshot.get("source_trade_date"))
    contract = rule_contract.metadata()
    return {"rule_version": contract["rule_version"],
            "rule_parameters_sha256": contract["rule_parameters_sha256"],
            "signal_date": rule_state.get("as_of") if active else None,
            "quote_snapshot_id": snapshot.get("quote_snapshot_id"),
            "rule_input": rule_state.get("rule_input") if rule_state else None,
            "earliest_hypothetical_fill": "信号日之后下一可核对交易日开盘" if active else None,
            "earliest_hypothetical_fill_date": None,
            "same_close_fill": False}
