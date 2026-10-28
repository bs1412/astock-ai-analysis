# -*- coding: utf-8 -*-
"""Holding exit alerts from completed daily closes; never places an order."""
import math
from datetime import date, datetime, timedelta

import pandas as pd

import stock_agent as sa
import signal_snapshot
import source_check
import rule_contract
import rule_inputs
from entry_signals import SHANGHAI, MAX_QUOTE_AGE_DAYS


TRAIL_ARM_PROFIT = 0.15
TRAIL_DRAWDOWN = 0.10
MA21_BREAK_DISTANCE = 0.005
MA21_CONFIRM_DAYS = 2
MAX_TRAIL_HISTORY_DAYS = 3650
MIN_RECENT_CALENDAR_DAYS = 180


def _date_value(value):
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _unavailable(reason):
    return {"status": "unavailable", "triggered": False, "reason": reason}


def history_start(today, acquired_on=None, minimum_calendar_days=MIN_RECENT_CALENDAR_DAYS):
    """Use one holding-history boundary for cards, alerts and paper evidence."""
    earliest = today - timedelta(days=minimum_calendar_days)
    acquired = _date_value(acquired_on)
    if acquired and 0 <= (today - acquired).days <= MAX_TRAIL_HISTORY_DAYS:
        earliest = min(earliest, acquired - timedelta(days=60))
    return earliest


def evaluate_exit_frame(frame, holding, today=None):
    today = today or datetime.now(SHANGHAI).date()
    acquired = _date_value(holding.get("acquired_on"))
    try:
        cost = float(holding.get("cost_price"))
    except (TypeError, ValueError):
        cost = math.nan
    result = {"status": "data_insufficient", "decision": "数据不足",
              "code": str(holding.get("code") or ""), "holding_id": holding.get("id"),
              "as_of": None, "acquired_on": acquired.isoformat() if acquired else None,
              "execution_warning": None,
              "ma21_break": _unavailable("完整日线不足"),
              "trailing_profit": _unavailable("缺少买入日期或持仓后完整历史"),
              "next_step": "等待完整行情，不根据本卡下单。",
              "price_basis_note": "历史收盘为前复权价；录入成本须已与当前股数、当前价格口径一致。公司行动后若未更新成本，盈亏门槛可能失真。"}
    if frame is None or frame.empty:
        return result
    df = frame.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    if "volume" in df:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df = (df.dropna(subset=["date", "close"])
          .loc[lambda rows: rows["close"].gt(0) & rows["date"].dt.date.lt(today)]
          .drop_duplicates("date", keep="last").sort_values("date").set_index("date"))
    if df.empty:
        return result
    result["as_of"] = df.index[-1].strftime("%Y-%m-%d")
    if "volume" in df and pd.notna(df["volume"].iloc[-1]) and df["volume"].iloc[-1] == 0:
        result["execution_warning"] = "最新完整日线成交量为0；可能停牌或数据异常，不能假定可卖出。"
    if (today - df.index[-1].date()).days > MAX_QUOTE_AGE_DAYS:
        result["status"] = "stale"
        result["decision"] = "行情过期"
        result["next_step"] = "先核实停牌或数据源延迟，再评估退出规则。"
        return result
    close = df["close"]
    if len(close) >= 22:
        ma21 = close.rolling(21).mean()
        recent_close = close.tail(MA21_CONFIRM_DAYS)
        recent_ma = ma21.tail(MA21_CONFIRM_DAYS)
        broken = bool((recent_close <= recent_ma * (1 - MA21_BREAK_DISTANCE)).all())
        result["ma21_break"] = {
            "status": "triggered" if broken else "waiting", "triggered": broken,
            "reason": f"最近{MA21_CONFIRM_DAYS}个已结束交易日收盘均低于各自21日均线至少{MA21_BREAK_DISTANCE * 100:g}%"
                      if broken else f"最近{MA21_CONFIRM_DAYS}个已结束交易日未同时低于各自21日均线至少{MA21_BREAK_DISTANCE * 100:g}%",
            "latest_close": round(float(close.iloc[-1]), 3),
            "latest_ma21": round(float(ma21.iloc[-1]), 3),
        }

    if acquired is None:
        result["trailing_profit"] = _unavailable("未填写买入日期；无法确定持仓后最高收盘价")
    elif acquired > df.index[-1].date():
        result["trailing_profit"] = _unavailable("买入日期晚于最新完整日线，尚无持仓后收盘历史")
    elif (today - acquired).days > MAX_TRAIL_HISTORY_DAYS:
        result["trailing_profit"] = _unavailable("买入日期距今超过3650个自然日的取数上限，无法确认完整持仓后高点")
    elif df.index[0].date() > acquired + timedelta(days=14):
        result["trailing_profit"] = _unavailable("行情起点晚于买入日期，无法确认持仓后高点")
    elif not math.isfinite(cost) or cost <= 0:
        result["trailing_profit"] = _unavailable("录入成本无效")
    else:
        after_buy = close.loc[close.index.date >= acquired]
        if after_buy.empty:
            result["trailing_profit"] = _unavailable("没有持仓后的完整收盘价")
        elif after_buy.index.to_series().diff().dt.days.gt(14).any():
            result["trailing_profit"] = _unavailable("持仓后行情存在超过14个自然日的空档，无法确认阶段最高收盘")
        else:
            peak = float(after_buy.max())
            peak_date = after_buy.idxmax().strftime("%Y-%m-%d")
            current = float(after_buy.iloc[-1])
            peak_profit = peak / cost - 1
            drawdown = 1 - current / peak
            armed = peak_profit > TRAIL_ARM_PROFIT
            triggered = armed and drawdown >= TRAIL_DRAWDOWN
            result["trailing_profit"] = {
                "status": "triggered" if triggered else ("armed" if armed else "waiting"),
                "triggered": triggered, "armed": armed,
                "reason": ("持仓后最高收盘价曾超过录入成本15%，最新收盘从该高点回撤至少10%"
                           if triggered else ("曾超过15%盈利门槛，但尚未从最高收盘回撤10%"
                                              if armed else "持仓后最高收盘尚未超过录入成本15%")),
                "peak_close": round(peak, 3), "peak_date": peak_date,
                "peak_profit_pct": round(peak_profit * 100, 2),
                "drawdown_from_peak_pct": round(drawdown * 100, 2),
                "current_profit_pct": round((current / cost - 1) * 100, 2),
            }

    triggered = result["ma21_break"]["triggered"] or result["trailing_profit"]["triggered"]
    incomplete = any(result[key]["status"] == "unavailable"
                     for key in ("ma21_break", "trailing_profit"))
    result["status"] = "risk_alert" if triggered else ("partial" if incomplete else "watching")
    result["decision"] = "风险退出提示" if triggered else ("部分信息不足" if incomplete else "继续观察")
    result["next_step"] = ("下一交易日先核实报价、T+1可卖数量、涨跌停和流动性，再决定是否执行；本卡不生成订单。"
                           if triggered else "继续观察下一条完整日线；若买入日期缺失，请补录后再检查移动止盈。")
    return result


def exit_signal(holding):
    today = datetime.now(SHANGHAI).date()
    earliest = history_start(today, holding.get("acquired_on"))
    frame = sa._kline_fast_for_user(holding["code"], earliest.strftime("%Y%m%d"), today.strftime("%Y%m%d"))
    cleaned, rejected = signal_snapshot.clean_frame(frame, today)
    result = evaluate_exit_frame(cleaned, holding, today=today)
    snapshot = signal_snapshot.data_snapshot(holding["code"], frame, cleaned, rejected,
                                             today, MAX_QUOTE_AGE_DAYS)
    if result["status"] == "risk_alert" and snapshot["current_signal_eligible"]:
        frame = source_check.with_secondary_check(holding["code"], frame, today)
        snapshot = signal_snapshot.data_snapshot(holding["code"], frame, cleaned, rejected,
                                                 today, MAX_QUOTE_AGE_DAYS)
    signal_snapshot.require_rule_dates(snapshot, (result, "risk_alert"))
    if not snapshot["current_signal_eligible"]:
        result.update({"status": "data_insufficient", "decision": "数据不足",
                       "reason": signal_snapshot.unavailable_reason(snapshot, MAX_QUOTE_AGE_DAYS),
                       "next_step": "先核对行情来源、日期和质量；退出条件计算仅供复核，不据此卖出。"})
    result["data_snapshot"] = snapshot
    result["rule_input"] = rule_inputs.evidence(holding["code"], cleaned, today, holding)
    result["signal_trace"] = signal_snapshot.signal_trace(result, snapshot, "risk_alert")
    result.update(rule_contract.metadata())
    result["data_source"] = snapshot["source"]
    result["source_retrieved_at"] = snapshot["retrieved_at"]
    return result
