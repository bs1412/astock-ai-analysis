# -*- coding: utf-8 -*-
"""Novice decision card and fixed-rule chronological holdout replay.

The replay is descriptive. A daily OHLCV bar cannot establish real execution.
"""
import math
import re
from datetime import datetime, timedelta

import pandas as pd

import entry_signals
import exit_signals
import signal_snapshot
import source_check
import rule_contract
import stock_agent as sa
import trade_constraints
import rule_inputs


HISTORY_CALENDAR_DAYS = 1460
MIN_HISTORY_BARS = 260
HOLDOUT_FRACTION = 0.40
HORIZON_SESSIONS = 10
SHARES = 100
BROKER_FEE_RATE = 0.001
MIN_COMMISSION = 5.0
SLIPPAGE_RATE = 0.0005
SELL_STAMP_RATE = 0.0005
OLD_SELL_STAMP_RATE = 0.001
STAMP_CHANGE_DATE = "2023-08-28"
MAX_GAP_DAYS = 14


def _clean_frame(frame, today):
    return signal_snapshot.clean_frame(frame, today)


def _date(row):
    return row["date"].date()


def _latest_bar_quality(df):
    """Compatibility wrapper for callers checking a previously selected row."""
    return signal_snapshot.latest_bar_quality(df.iloc[-1] if df is not None and not df.empty else None)


def _data_snapshot(code, raw, df, rejected, quote_date, today):
    return signal_snapshot.data_snapshot(
        code, raw, df, rejected, today, entry_signals.MAX_QUOTE_AGE_DAYS)


def _bar_price(row, side):
    values = (row["open"], row["high"], row["low"], row["close"], row["volume"])
    if any(pd.isna(value) or not math.isfinite(float(value)) for value in values):
        return None, "开高低收或成交量缺失"
    opening, high, low, close, volume = map(float, values)
    if min(opening, high, low, close) <= 0 or volume <= 0:
        return None, "价格或成交量无效，可能停牌或数据缺失"
    if high < max(opening, close) or low > min(opening, close) or high == low:
        return None, "价格范围异常或全天单一价格，无法确认可成交"
    raw = opening * (1 + SLIPPAGE_RATE if side == "buy" else 1 - SLIPPAGE_RATE)
    price = (math.ceil(raw * 100 - 1e-9) if side == "buy"
             else math.floor(raw * 100 + 1e-9)) / 100
    if price <= 0 or price > high + 0.001 or price < low - 0.001:
        return None, "滑点后价格超出当日价格范围"
    return price, None


def _cost(amount, side, trade_date=None):
    commission = max(MIN_COMMISSION, amount * BROKER_FEE_RATE)
    stamp_rate = (OLD_SELL_STAMP_RATE if trade_date is not None and str(trade_date) < STAMP_CHANGE_DATE
                  else SELL_STAMP_RATE)
    stamp = amount * stamp_rate if side == "sell" else 0.0
    return commission + stamp


def _net_return_pct(buy_price, sell_price, sell_date=None, shares=SHARES):
    paid = shares * buy_price + _cost(shares * buy_price, "buy")
    received = shares * sell_price - _cost(shares * sell_price, "sell", sell_date)
    return (received / paid - 1) * 100


def _scenario(close, code=None):
    if not math.isfinite(close) or close <= 0:
        return None
    quantity_policy = trade_constraints.profile(code) if code is not None else None
    if code is not None and quantity_policy is None:
        return None
    shares = quantity_policy["minimum_buy_shares"] if quantity_policy else SHARES
    buy = math.ceil(close * (1 + SLIPPAGE_RATE) * 100 - 1e-9) / 100
    target_sell = math.floor(close * 1.10 * (1 - SLIPPAGE_RATE) * 100 + 1e-9) / 100
    risk_sell = math.floor(close * 0.95 * (1 - SLIPPAGE_RATE) * 100 + 1e-9) / 100
    return {"reference_close": round(close, 3), "assumed_buy_price": buy,
            "gross_target_price": round(close * 1.10, 2),
            "gross_risk_price": round(close * 0.95, 2),
            "net_if_gross_up_10_pct": round(_net_return_pct(buy, target_sell, shares=shares), 2),
            "net_if_gross_down_5_pct": round(_net_return_pct(buy, risk_sell, shares=shares), 2),
            "shares": shares, "quantity_policy": quantity_policy,
            "broker_fee_pct_per_side": BROKER_FEE_RATE * 100,
            "min_commission": MIN_COMMISSION, "slippage_pct_per_side": SLIPPAGE_RATE * 100,
            "sell_stamp_pct": SELL_STAMP_RATE * 100}


def _holdout_replay(df, today, rejected_bars):
    n = len(df) if df is not None else 0
    base = {"status": "insufficient", "method": "参数固定后的后40%已结束、有日期和收盘的日线时间留出回放",
            "total_complete_bars": n, "rejected_bars": rejected_bars,
            "holdout_start": None, "holdout_end": None, "holdout_bars": 0,
            "evaluated_days": 0, "evaluated_through": None,
            "volume_price_prefilter_days": 0,
            "entry_signal_count": 0, "entry_unfilled_count": 0,
            "entry_unfilled_reasons": {}, "completed_trades": 0,
            "risk_exit_count": 0, "timeout_exit_count": 0,
            "unfinished_count": 0, "net_positive_count": 0, "net_target_10_count": 0,
            "net_positive_pct": None, "net_target_10_pct": None,
            "median_net_return_pct": None, "worst_net_return_pct": None,
            "first_signal_date": None, "last_signal_date": None,
            "interrupted_reason": None, "evidence_grade": "未验证",
            "note": "旧的10日目标情景与本规则不同，不可混为一项命中率。"}
    if n < MIN_HISTORY_BARS:
        base["note"] = f"可用收盘日线仅{n}条，至少需{MIN_HISTORY_BARS}条才能做本项时间留出回放。"
        return base
    start = max(entry_signals.MIN_DAILY_ROWS, math.floor(n * (1 - HOLDOUT_FRACTION)))
    base["holdout_start"] = df["date"].iloc[start].strftime("%Y-%m-%d")
    base["holdout_end"] = df["date"].iloc[-1].strftime("%Y-%m-%d")
    base["holdout_bars"] = n - start
    close = df["close"]
    volume = df["volume"]
    previous_mean = volume.shift(1).rolling(entry_signals.VOLUME_BASE_DAYS).mean()
    prefilter = (close > close.shift(1)) & (volume >= entry_signals.VOLUME_MULTIPLE * previous_mean)
    returns = []
    i = start
    while i < n:
        base["evaluated_days"] += 1
        base["evaluated_through"] = df["date"].iloc[i].strftime("%Y-%m-%d")
        if not bool(prefilter.iloc[i]):
            i += 1
            continue
        base["volume_price_prefilter_days"] += 1
        signal_day = _date(df.iloc[i])
        cutoff = signal_day - timedelta(days=entry_signals.LOOKBACK_CALENDAR_DAYS)
        history = df.loc[(df["date"].dt.date >= cutoff) & (df.index <= i)]
        signal = entry_signals.evaluate_entry_frame(history, today=signal_day + timedelta(days=1))
        if signal["status"] != "candidate":
            i += 1
            continue
        base["entry_signal_count"] += 1
        signal_text = signal_day.isoformat()
        base["first_signal_date"] = base["first_signal_date"] or signal_text
        base["last_signal_date"] = signal_text
        if i + 1 >= n:
            base["unfinished_count"] += 1
            i += 1
            continue
        buy_day = _date(df.iloc[i + 1])
        if (buy_day - signal_day).days > MAX_GAP_DAYS:
            reason = "信号至下一日线间隔超过14天"
            base["entry_unfilled_count"] += 1
            base["entry_unfilled_reasons"][reason] = base["entry_unfilled_reasons"].get(reason, 0) + 1
            i += 1
            continue
        buy_price, reason = _bar_price(df.iloc[i + 1], "buy")
        if reason:
            base["entry_unfilled_count"] += 1
            base["entry_unfilled_reasons"][reason] = base["entry_unfilled_reasons"].get(reason, 0) + 1
            i += 1
            continue
        exit_index = i + HORIZON_SESSIONS + 1
        exit_reason = "10个交易日观察期到期"
        interrupted = False
        for j in range(i + 1, min(i + HORIZON_SESSIONS + 1, n)):
            if (_date(df.iloc[j]) - _date(df.iloc[j - 1])).days > MAX_GAP_DAYS:
                base["interrupted_reason"] = "持仓期间日线间隔超过14天，无法核实退出路径"
                interrupted = True
                break
            holding = {"code": "replay", "cost_price": buy_price + _cost(SHARES * buy_price, "buy") / SHARES,
                       "acquired_on": buy_day.isoformat()}
            state = exit_signals.evaluate_exit_frame(
                df.iloc[:j + 1], holding, today=_date(df.iloc[j]) + timedelta(days=1))
            if state["status"] == "risk_alert":
                exit_index = j + 1
                exit_reason = "五步规则风险退出"
                break
        if interrupted:
            base["unfinished_count"] += 1
            break
        if exit_index >= n or (_date(df.iloc[exit_index]) - _date(df.iloc[exit_index - 1])).days > MAX_GAP_DAYS:
            base["unfinished_count"] += 1
            base["interrupted_reason"] = "退出日线缺失或间隔超过14天"
            break
        sell_price, reason = _bar_price(df.iloc[exit_index], "sell")
        if reason:
            base["unfinished_count"] += 1
            base["interrupted_reason"] = f"退出价无法按假设成交：{reason}"
            break
        net = _net_return_pct(buy_price, sell_price, _date(df.iloc[exit_index]))
        returns.append(net)
        if exit_reason == "五步规则风险退出":
            base["risk_exit_count"] += 1
        else:
            base["timeout_exit_count"] += 1
        i = exit_index + 1
    base["completed_trades"] = len(returns)
    base["net_positive_count"] = sum(value > 0 for value in returns)
    base["net_target_10_count"] = sum(value >= 10 for value in returns)
    if returns:
        ordered = sorted(returns)
        mid = len(ordered) // 2
        median = ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2
        base["median_net_return_pct"] = round(median, 2)
        base["worst_net_return_pct"] = round(min(returns), 2)
        base["net_positive_pct"] = round(base["net_positive_count"] / len(returns) * 100, 1)
        base["net_target_10_pct"] = round(base["net_target_10_count"] / len(returns) * 100, 1)
    base["status"] = "descriptive" if returns else "no_completed_trades"
    base["note"] = ("本规则在留出时间段的描述性回放；逐股不重叠交易，信号使用截至当日收盘的信息，"
                    "买卖均用下一交易日开盘加不利滑点。忽略真实盘口、涨跌停排队和公司行动；"
                    "当前前复权序列可能带入事后调整，不能据此断言未来收益或真实可成交。")
    return base


def build(code, holding=None, include_holdout=True):
    code = str(code).strip()
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("请输入6位数字股票代码")
    today = datetime.now(entry_signals.SHANGHAI).date()
    quick = not include_holdout and holding is None
    start = today - timedelta(days=(entry_signals.LOOKBACK_CALENDAR_DAYS
                                    if quick else HISTORY_CALENDAR_DAYS))
    if holding is not None:
        start = min(start, exit_signals.history_start(today, holding.get("acquired_on")))
    source_check.prefetch_sina(code,today,minimum_remaining_seconds=source_check.PREFETCH_PRIMARY_RESERVE_SECONDS)
    raw = sa._kline_fast_for_user(code, start.strftime("%Y%m%d"), today.strftime("%Y%m%d"))
    df, rejected = _clean_frame(raw, today)
    live_start = today - timedelta(days=entry_signals.LOOKBACK_CALENDAR_DAYS)
    live_frame = df.loc[df["date"].dt.date >= live_start] if df is not None else None
    entry = entry_signals.evaluate_entry_frame(live_frame, today=today)
    exit_state = exit_signals.evaluate_exit_frame(df, holding, today=today) if holding else None
    validation = None if quick else _holdout_replay(df, today, rejected)
    price = float(df["close"].iloc[-1]) if df is not None else None
    quote_date = df["date"].iloc[-1].strftime("%Y-%m-%d") if df is not None else None
    snapshot = _data_snapshot(code, raw, df, rejected, quote_date, today)
    if snapshot["current_signal_eligible"]:
        raw = source_check.with_secondary_check(code, raw, today)
        snapshot = _data_snapshot(code, raw, df, rejected, quote_date, today)
    signal_snapshot.require_rule_dates(snapshot, (entry, "candidate"), (exit_state, "risk_alert"))
    quote_date = snapshot["source_trade_date"]
    if not snapshot["current_signal_eligible"]:
        entry.update({"status": "data_insufficient", "decision": "数据不足",
                      "reason": signal_snapshot.unavailable_reason(snapshot, entry_signals.MAX_QUOTE_AGE_DAYS),
                      "next_step": "等待有效、完整且日期未过期的日线，再重新核对入场条件。"})
        if exit_state is not None:
            exit_state.update({"status": "data_insufficient", "decision": "数据不足",
                               "reason": signal_snapshot.unavailable_reason(
                                   snapshot, entry_signals.MAX_QUOTE_AGE_DAYS),
                               "next_step": "先核对行情来源、日期和质量，再复核退出条件与可卖数量。"})
    entry["rule_input"] = rule_inputs.evidence(code, live_frame, today)
    entry["signal_trace"] = signal_snapshot.signal_trace(entry, snapshot, "candidate")
    entry.update(rule_contract.metadata())
    if exit_state is not None:
        exit_state["rule_input"] = rule_inputs.evidence(code, df, today, holding)
        exit_state["signal_trace"] = signal_snapshot.signal_trace(exit_state, snapshot, "risk_alert")
        exit_state.update(rule_contract.metadata())
    if holding and not snapshot["current_signal_eligible"]:
        label, next_step = "持仓信息不足", "先核对行情来源、日期和质量，再复核退出条件与可卖数量。"
    elif holding and exit_state["status"] == "risk_alert":
        label, next_step = "持仓风险待核实", "核对可卖数量、次日价格和流动性；不要按信号日收盘价假定已卖出。"
    elif holding and exit_state["status"] in ("stale", "data_insufficient", "partial"):
        label, next_step = "持仓信息不足", "先补齐买入日期、成本口径及行情，再判断退出条件。"
    elif holding:
        label, next_step = "持仓继续观察", "继续跟踪两项退出条件和下一条完整日线。"
    elif entry["status"] == "candidate":
        label, next_step = "候选观察，尚未验证", "先核对下一交易日报价、板块最低买入数量、预算和交易限制；目前不生成买入指令。"
    elif entry["status"] in ("stale", "data_insufficient"):
        label, next_step = "数据不足", "等待有效完整日线，不据此操作。"
    else:
        label, next_step = "等待条件", "三项入场条件未同时满足，继续观察。"
    return {"code": code, "status": label, "next_step": next_step,
            **rule_contract.metadata(),
            "request_cache_state": raw.attrs.get("request_cache_state", "unknown") if raw is not None else "unknown",
            "as_of": quote_date, "data_source": raw.attrs.get("source") if raw is not None else None,
            "source_retrieved_at": raw.attrs.get("source_retrieved_at") if raw is not None else None,
            "data_snapshot": snapshot,
            "entry": entry, "exit": exit_state, "holding": bool(holding),
            "cost_scenario": _scenario(price, code) if snapshot["current_signal_eligible"] and price is not None else None,
            "scenario_unavailable_reason": (
                (None if trade_constraints.profile(code) is not None else "证券身份或板块交易单位未核实，暂不生成数量费用情景") if snapshot["current_signal_eligible"] else
                signal_snapshot.unavailable_reason(snapshot, entry_signals.MAX_QUOTE_AGE_DAYS)),
            "holdout": validation, "holdout_pending": quick,
            "risk_note": "图示成本按所列板块最低股数、固定佣金和滑点假设，与模拟账本可调参数不联动；不含过户费、分红送转、涨跌停排队和真实盘口。历史留出回放仍固定100股，未逐股还原板块及历史交易规则。多批买入或公司行动后成本与高点可能不可比。回放不是未来概率或买卖指令。"}


def replay(code):
    """独立历史描述性回放，结果不覆盖当前规则或快照状态。"""
    code = str(code).strip()
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("请输入6位数字股票代码")
    today = datetime.now(entry_signals.SHANGHAI).date()
    start = today - timedelta(days=HISTORY_CALENDAR_DAYS)
    raw = sa._kline_fast_for_user(code, start.strftime("%Y%m%d"), today.strftime("%Y%m%d"))
    df, rejected = _clean_frame(raw, today)
    return {"code": code, "holdout": _holdout_replay(df, today, rejected),
            "as_of": df["date"].iloc[-1].strftime("%Y-%m-%d") if df is not None else None,
            "data_source": raw.attrs.get("source") if raw is not None else None,
            "source_retrieved_at": raw.attrs.get("source_retrieved_at") if raw is not None else None,
            "request_cache_state": raw.attrs.get("request_cache_state", "unknown") if raw is not None else "unknown"}
