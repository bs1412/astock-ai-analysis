# -*- coding: utf-8 -*-
"""Five-step rule, entry stages only. No same-close execution or profitability claim."""
import math
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

import stock_agent as sa
import signal_snapshot
import source_check
import rule_contract
import rule_inputs


SHANGHAI = ZoneInfo("Asia/Shanghai")
LOOKBACK_CALENDAR_DAYS = 400
MIN_DAILY_ROWS = 120
WEEKLY_MA_WEEKS = 13
WEEKLY_SLOPE_LAG_WEEKS = 4
PULLBACK_WINDOW_DAYS = 5
PULLBACK_DISTANCE = 0.02
ZERO_AXIS_WINDOW_DAYS = 3
ZERO_AXIS_DISTANCE = 0.005
VOLUME_BASE_DAYS = 20
VOLUME_MULTIPLE = 1.3
MAX_QUOTE_AGE_DAYS = 7


def _condition(key, label, passed, reason):
    return {"key": key, "label": label, "passed": bool(passed), "reason": reason}


def evaluate_entry_frame(frame, today=None):
    """Use only completed mainland trading days and completed Friday weeks."""
    today = today or datetime.now(SHANGHAI).date()
    base = {"status": "data_insufficient", "decision": "数据不足", "as_of": None,
            "weekly_as_of": None, "conditions": [], "reason": None,
            "next_step": "等待完整日线和周线；不要据此下单。",
            "assumptions": {"adjustment": "前复权日线", "daily_cutoff": "仅使用今天以前的交易日",
                            "weekly_cutoff": "仅使用已结束的周五周线",
                            "same_close_fill": False}}
    if frame is None or frame.empty:
        base["reason"] = "历史日线未取得"
        return base
    df = frame.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df = (df.dropna(subset=["date", "close", "volume"])
          .loc[lambda rows: rows["close"].gt(0) & rows["volume"].ge(0)
               & rows["date"].dt.date.lt(today)]
          .drop_duplicates("date", keep="last").sort_values("date").set_index("date"))
    if df.empty:
        base["reason"] = "没有已结束且数值有效的日线"
        return base
    base["as_of"] = df.index[-1].strftime("%Y-%m-%d")
    if (today - df.index[-1].date()).days > MAX_QUOTE_AGE_DAYS:
        base["status"] = "stale"
        base["decision"] = "数据过期"
        base["reason"] = f"最新完整日线距今超过{MAX_QUOTE_AGE_DAYS}个自然日"
        return base
    if len(df) < MIN_DAILY_ROWS:
        base["reason"] = f"只有{len(df)}条完整日线，至少需要{MIN_DAILY_ROWS}条"
        return base

    close, volume = df["close"], df["volume"]
    weekly_close = close.resample("W-FRI").last()
    weekly_date = pd.Series(df.index, index=df.index).resample("W-FRI").last()
    completed = weekly_close.index.date < today
    weekly_close = weekly_close.loc[completed]
    weekly_date = weekly_date.loc[completed]
    required_weeks = WEEKLY_MA_WEEKS + WEEKLY_SLOPE_LAG_WEEKS
    if len(weekly_close) < required_weeks or weekly_close.tail(required_weeks).isna().any():
        base["reason"] = f"连续完整周线不足{required_weeks}周，无法判断13周均线方向"
        return base
    base["weekly_as_of"] = weekly_date.iloc[-1].strftime("%Y-%m-%d")
    ma13 = weekly_close.rolling(WEEKLY_MA_WEEKS).mean()
    week_ok = bool(ma13.iloc[-1] > ma13.iloc[-1 - WEEKLY_SLOPE_LAG_WEEKS]
                   and weekly_close.iloc[-1] >= ma13.iloc[-1])

    ma21 = close.rolling(21).mean()
    recent_distance = ((close / ma21) - 1).abs().tail(PULLBACK_WINDOW_DAYS)
    pullback_ok = bool(recent_distance.le(PULLBACK_DISTANCE).any()
                       and close.iloc[-1] >= ma21.iloc[-1])
    dif = close.ewm(span=12, adjust=False).mean() - close.ewm(span=26, adjust=False).mean()
    dea = dif.ewm(span=9, adjust=False).mean()
    zero_ok = bool((dif.abs() / close).tail(ZERO_AXIS_WINDOW_DAYS).le(ZERO_AXIS_DISTANCE).any())
    cross_ok = bool(dif.iloc[-1] > dea.iloc[-1] and dif.iloc[-2] <= dea.iloc[-2]
                    and dif.iloc[-1] > dif.iloc[-2])
    macd_ok = pullback_ok and zero_ok and cross_ok

    previous_volume_mean = volume.shift(1).rolling(VOLUME_BASE_DAYS).mean().iloc[-1]
    volume_ok = bool(math.isfinite(previous_volume_mean) and previous_volume_mean > 0
                     and volume.iloc[-1] >= VOLUME_MULTIPLE * previous_volume_mean)
    price_ok = bool(close.iloc[-1] > close.iloc[-2])
    volume_price_ok = volume_ok and price_ok
    base["conditions"] = [
        _condition("weekly_trend", "13周趋势", week_ok,
                   f"截至{base['weekly_as_of']}，周收盘{weekly_close.iloc[-1]:.2f}，13周均线{ma13.iloc[-1]:.2f}；与4周前均线比较，须向上且周收盘不低于均线"),
        _condition("pullback_macd", "回踩与MACD", macd_ok,
                   f"近5日收盘距21日线不超过2%且最新收盘站在线上：{'是' if pullback_ok else '否'}；近3日DIF绝对值/收盘价不超过0.5%：{'是' if zero_ok else '否'}；最新完整日DIF上穿DEA且DIF上行：{'是' if cross_ok else '否'}"),
        _condition("volume_price", "量价确认", volume_price_ok,
                   f"最新成交量为前20日均量的{(volume.iloc[-1] / previous_volume_mean):.2f}倍"
                   if math.isfinite(previous_volume_mean) and previous_volume_mean > 0
                   else "前20日均量无效"),
    ]
    base["conditions"][-1]["reason"] += f"；收盘较前日上涨：{'是' if price_ok else '否'}；需同时达到1.3倍均量"
    candidate = all(item["passed"] for item in base["conditions"])
    base["status"] = "candidate" if candidate else "waiting"
    base["decision"] = "候选观察" if candidate else "等待条件"
    base["reason"] = ("三项条件同时满足；仍需核实下一交易日行情与可成交性"
                      if candidate else "至少一项入场条件未满足")
    base["next_step"] = ("下一交易日重新检查价格、交易限制和风险预算；本卡不生成订单。"
                         if candidate else "继续观察，不因单项满足就追价。")
    return base


def entry_signal(code):
    code = str(code).strip()
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("请输入6位数字股票代码")
    today = datetime.now(SHANGHAI).date()
    start = (today - timedelta(days=LOOKBACK_CALENDAR_DAYS)).strftime("%Y%m%d")
    frame = sa._kline_fast_for_user(code, start, today.strftime("%Y%m%d"))
    cleaned, rejected = signal_snapshot.clean_frame(frame, today)
    result = evaluate_entry_frame(cleaned, today=today)
    snapshot = signal_snapshot.data_snapshot(code, frame, cleaned, rejected, today, MAX_QUOTE_AGE_DAYS)
    if result["status"] == "candidate" and snapshot["current_signal_eligible"]:
        frame = source_check.with_secondary_check(code, frame, today)
        snapshot = signal_snapshot.data_snapshot(code, frame, cleaned, rejected, today, MAX_QUOTE_AGE_DAYS)
    signal_snapshot.require_rule_dates(snapshot, (result, "candidate"))
    if not snapshot["current_signal_eligible"]:
        result.update({"status": "data_insufficient", "decision": "数据不足",
                       "reason": signal_snapshot.unavailable_reason(snapshot, MAX_QUOTE_AGE_DAYS),
                       "next_step": "先核对行情来源、日期和质量；条件计算仅供复核，不据此下单。"})
    result["code"] = code
    result["request_cache_state"] = frame.attrs.get("request_cache_state", "unknown") if frame is not None else "unknown"
    result["data_snapshot"] = snapshot
    result["rule_input"] = rule_inputs.evidence(code, cleaned, today)
    result["signal_trace"] = signal_snapshot.signal_trace(result, snapshot, "candidate")
    result.update(rule_contract.metadata())
    result["data_source"] = snapshot["source"]
    result["source_retrieved_at"] = snapshot["retrieved_at"]
    return result
