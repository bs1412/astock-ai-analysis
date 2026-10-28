# -*- coding: utf-8 -*-
"""模拟交易账本：独立记录信号、下一完整日线推算成交与未成交原因。"""
import json
import hashlib
import os
import time
import threading
import concurrent.futures
import copy
import math
import uuid
from datetime import datetime, timedelta

import entry_signals
import exit_signals
import screener
import stock_agent as sa
import signal_snapshot
import source_check
import rule_contract
import rule_inputs
import trade_constraints

BASE = os.path.dirname(os.path.abspath(__file__))
FILE = os.path.join(BASE, "paper.json")
DEFAULTS = {"cash": 100000.0, "initial_cash": 100000.0, "positions": [], "trades": [],
            "pending_orders": [], "order_events": [], "ten_day_observations": [], "five_day_observations": [],
            "rules": {"buy_top_n": 2, "buy_min_score": 65, "max_position_pct": 20,
                      "max_trades_per_day": 4, "broker_fee_pct": 0.1,
                      "slippage_pct": 0.05, "min_commission": 5.0}}
_score_cache = None
_CHECK_LOCK = threading.Lock()
OBSERVATION_SESSIONS = 10
OBSERVATION_TARGET_NET = 0.10
OBSERVATION_RISK_NET = -0.05
OBSERVATION_MAX_GAP_DAYS = 14
REVISION_AUDIT_INTERVAL_DAYS = 7
REVISION_AUDIT_MAX_PER_CHECK = 3


class LedgerReadError(ValueError):
    """Existing ledger could not be read; callers must not initialize over it."""


def _observation_policy(sessions=OBSERVATION_SESSIONS):
    return {"version": "close_net_v1", "sessions": sessions,
            "target_net_pct": OBSERVATION_TARGET_NET * 100,
            "risk_net_pct": OBSERVATION_RISK_NET * 100,
            "max_gap_calendar_days": OBSERVATION_MAX_GAP_DAYS,
            "price_basis": "completed_daily_close", "sell_is_estimate": True}


def _observation_items(d):
    for sessions, key in ((10, "ten_day_observations"), (5, "five_day_observations")):
        for item in d.get(key, []):
            yield sessions, item


def load():
    if os.path.isfile(FILE):
        try:
            with open(FILE, encoding="utf-8") as f:
                d = json.load(f)
                if not isinstance(d, dict) or not isinstance(d.get("rules", {}), dict):
                    raise ValueError("invalid ledger structure")
                for key, default in DEFAULTS.items():
                    if isinstance(default, list) and key in d and not isinstance(d[key], list):
                        raise ValueError("invalid ledger record list")
                for key in ("cash", "initial_cash"):
                    if key in d and (isinstance(d[key], bool) or not isinstance(d[key], (int, float))
                                     or not math.isfinite(d[key]) or d[key] < 0):
                        raise ValueError("invalid ledger cash")
                return {**copy.deepcopy(DEFAULTS), **d, "rules": {**DEFAULTS["rules"], **d.get("rules", {})}}
        except (OSError, ValueError, TypeError, OverflowError) as error:
            raise LedgerReadError("模拟账本无法读取或结构不合法；原文件保留，未自动重置，请先核对或恢复记录") from error
    return copy.deepcopy(DEFAULTS)


def save(d):
    temp = FILE + ".tmp"
    with open(temp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)
    os.replace(temp, FILE)


def reset():
    if not _CHECK_LOCK.acquire(blocking=False):
        raise RuntimeError("模拟检查正在进行，暂不能重置")
    try:
        save(copy.deepcopy(DEFAULTS))
    finally:
        _CHECK_LOCK.release()


def update_rules(values):
    if not _CHECK_LOCK.acquire(blocking=False):
        raise RuntimeError("模拟检查正在进行，暂不能修改规则")
    try:
        d = load()
        d["rules"].update(values)
        save(d)
        return d["rules"]
    finally:
        _CHECK_LOCK.release()


def _universe_scores(progress=None):
    global _score_cache
    coverage_count = screener.cache_coverage("full")["cached_count"]
    today = datetime.now(entry_signals.SHANGHAI).strftime("%Y-%m-%d")
    if (_score_cache and _score_cache[2] == coverage_count and _score_cache[3] == today
            and time.time() - _score_cache[0] < 900):
        return _score_cache[1]
    def screen_progress(done, total):
        if progress:
            progress("scan", done, total, f"正在更新模拟策略候选行情 {done}/{total}")

    rows = screener.screen(universe="full", refresh_missing=False)
    if not rows:
        rows = screener.screen(progress=screen_progress)
    elif progress:
        progress("scan", len(rows), len(rows), "已从当日全市场缓存读取候选行情")
    # Ranking data from an incomplete current day would select past signals with future information.
    rows = [r for r in rows if r.get("as_of") and r["as_of"] < today]
    _score_cache = (time.time(), rows, coverage_count, today)
    return rows


def _evidence(code, today, acquired_on=None, verify_secondary=False, with_raw=False):
    """Apply the same current-quote gate used by decision/watch/holding cards."""
    start = exit_signals.history_start(today, acquired_on,
                                      minimum_calendar_days=entry_signals.LOOKBACK_CALENDAR_DAYS)
    raw = sa._kline_fast(code, start.strftime("%Y%m%d"), today.strftime("%Y%m%d"))
    frame, rejected = signal_snapshot.clean_frame(raw, today)
    snapshot = signal_snapshot.data_snapshot(code, raw, frame, rejected, today,
                                             entry_signals.MAX_QUOTE_AGE_DAYS)
    if verify_secondary and snapshot["current_signal_eligible"]:
        raw = source_check.with_secondary_check(code, raw, today)
        snapshot = signal_snapshot.data_snapshot(code, raw, frame, rejected, today,
                                                 entry_signals.MAX_QUOTE_AGE_DAYS)
    return (frame, snapshot, raw, rejected) if with_raw else (frame, snapshot)


def _mark(code, today):
    frame, snapshot = _evidence(code, today)
    if frame is None or not snapshot["current_signal_eligible"]:
        return {"price": None, "date": snapshot["source_trade_date"], "ma10": None,
                "source": snapshot["source"], "retrieved_at": snapshot["retrieved_at"],
                "stale": snapshot["status"] == "stale"}
    closes = frame["close"]
    latest = frame["date"].iloc[-1]
    return {"price": round(float(closes.iloc[-1]), 3),
            "date": latest.strftime("%Y-%m-%d"),
            "ma10": round(float(closes.tail(10).mean()), 3) if len(closes) >= 10 else None,
            "source": frame.attrs.get("source"),
            "retrieved_at": frame.attrs.get("source_retrieved_at"),
            "stale": (today - latest.date()).days > entry_signals.MAX_QUOTE_AGE_DAYS}


def valuation_id(d):
    """Bind read-only valuation to this exact cash/position content, not a clock."""
    value={'cash':d['cash'],'initial_cash':d['initial_cash'],
           'positions':[{k:p.get(k) for k in ('id','code','shares','avg_cost','acquired_on')} for p in d['positions']]}
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def valuation(d=None):
    """Public quotes value a private position snapshot; never fills or saves it."""
    import holdings
    import market_clock
    d=copy.deepcopy(d if d is not None else load())
    identifier=valuation_id(d)
    def value_position(pair):
        index,p=pair
        try:quote=holdings.public_quote(p['code'])
        except Exception:
            quote={'code':p['code'],'price':None,'source':'tencent','source_time':None,'retrieved_at':None,
                   'price_evidence':market_clock.quote_evidence(None)}
        price=quote['price'];evidence=quote['price_evidence']
        valid=(price is not None and math.isfinite(float(price)) and price>0 and evidence['usable_for_valuation'])
        cost=float(p['avg_cost']);shares=p['shares']
        return {**quote,'position_index':index,'shares':shares,'valuation_available':bool(valid),
            'market_value':round(price*shares,2) if valid else None,
            'pnl':round((price-cost)*shares,2) if valid else None,
            'pnl_pct':round((price/cost-1)*100,2) if valid and cost>0 else None}
    if d['positions']:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8,len(d['positions']))) as pool:
            rows=list(pool.map(value_position,enumerate(d['positions'])))
    else:rows=[]
    complete=all(row['valuation_available'] for row in rows)
    position_value=round(sum(row['market_value'] for row in rows),2) if complete else None
    total=round(d['cash']+position_value,2) if complete else None
    return {'valuation_id':identifier,'rows':rows,'cash':round(d['cash'],2),'position_value':position_value,
        'total_assets':total,'pnl':round(total-d['initial_cash'],2) if complete else None,
        'pnl_pct':round((total/d['initial_cash']-1)*100,2) if complete and d['initial_cash'] else None,
        'valuation_incomplete':not complete,'market_time':market_clock.snapshot(),
        'note':'按所列公开报价时刻估算的持仓参考，不含未来退出费用，公司行动/成本口径另核对；不写账本、不触发成交、不改变五日/十日观察。'}


def summary(d=None):
    d = d if d is not None else load()
    today = datetime.now(entry_signals.SHANGHAI).date()
    def summarize_position(p):
        try:
            quote = _mark(p["code"], today)
        except Exception:
            quote = {"price": None, "date": None, "ma10": None, "source": None,
                     "retrieved_at": None, "stale": False}
        price = quote["price"]
        cost = float(p["avg_cost"])
        valid_price = price is not None and not quote["stale"]
        mv = (price if valid_price else cost) * p["shares"]
        pnl = (price - cost) * p["shares"] if valid_price else None
        return {**p, "name": p.get("name") or p["code"], "price": price,
                "price_date": quote["date"], "price_source": quote["source"],
                "price_retrieved_at": quote["retrieved_at"],
                "valuation_estimated": not valid_price, "quote_stale": quote["stale"],
                "ma10": quote["ma10"],
                "market_value": round(mv, 2), "pnl": round(pnl, 2) if pnl is not None else None,
                "pnl_pct": round((price / cost - 1) * 100, 2) if valid_price and cost else None}
    if d["positions"]:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(d["positions"]))) as pool:
            positions = list(pool.map(summarize_position, d["positions"]))
    else:
        positions = []
    pos_val = sum(p["market_value"] for p in positions)
    total = d["cash"] + pos_val
    init = d["initial_cash"]
    unknown = any(p["valuation_estimated"] for p in positions)
    observations = d.get("ten_day_observations", [])
    five_observations = d.get("five_day_observations", [])
    outcomes = ("pending_fill", "observing", "target_observed", "risk_observed",
                "timeout_observed", "unfilled", "data_insufficient")
    counts = {status: sum(item.get("status") == status for item in observations)
              for status in outcomes}
    five_counts = {status: sum(item.get("status") == status for item in five_observations)
                   for status in outcomes}
    five_ids = {item.get("intent_id") for item in five_observations if item.get("intent_id")}
    audit_counts = {status: sum(item.get("revision_audit_status") == status
                                for item in observations)
                    for status in ("baseline", "consistent", "source_changed", "revised",
                                   "unavailable", "missing_basis")}
    five_audit_counts = {status: sum(item.get("revision_audit_status") == status
                                     for item in five_observations)
                         for status in audit_counts}
    return {"valuation_id":valuation_id(d),"valuation_basis":"completed_daily_reference_not_live_quote",
            "cash": round(d["cash"], 2), "positions": positions, "position_value": round(pos_val, 2),
            "total_assets": round(total, 2), "pnl": None if unknown else round(total - init, 2),
            "pnl_pct": None if unknown or not init else round((total / init - 1) * 100, 2),
            "valuation_incomplete": unknown, "rules": d["rules"], "trade_count": len(d["trades"]),
            "trades": d["trades"][-50:][::-1], "pending_orders": d["pending_orders"][-30:][::-1],
            "order_events": d["order_events"][-50:][::-1],
            "ten_day_observations": observations[-50:][::-1],
            "ten_day_denominator": len(observations),
            "ten_day_status_counts": counts,
            "five_day_observations": five_observations[-50:][::-1],
            "five_day_denominator": len(five_observations),
            "five_day_status_counts": five_counts,
            "five_day_revision_audit_counts": five_audit_counts,
            "five_day_legacy_missing_count": sum(not item.get("intent_id") or item["intent_id"] not in five_ids
                                                 for item in observations),
            "revision_audit_counts": audit_counts,
            "quality_rejection_count": sum(event.get("status") == "quality_rejected"
                                            for event in d["order_events"]),
            "legacy_trade_count": sum(not trade.get("rule_version") for trade in d["trades"])}


def _legacy_acquired(p, trades):
    if p.get("acquired_on"):
        return p["acquired_on"]
    for trade in reversed(trades):
        if trade.get("code") == p.get("code"):
            return trade.get("date") if trade.get("action") == "buy" else None
    return None


def _costs(amount, side, rules):
    commission = max(float(rules["min_commission"]), amount * float(rules["broker_fee_pct"]) / 100)
    stamp = amount * 0.0005 if side == "sell" else 0.0
    return round(commission, 2), round(stamp, 2)


def _observation(d, order, sessions=10):
    if not order.get("intent_id"):
        return None
    key = "five_day_observations" if sessions == 5 else "ten_day_observations"
    return next((item for item in d.get(key, [])
                 if item.get("intent_id") == order.get("intent_id")), None)


def _order_observations(d, order):
    if not order.get("intent_id"):
        return []
    return [item for _, item in _observation_items(d)
            if item.get("intent_id") == order.get("intent_id")]


def _register_observation(d, order):
    if order["action"] != "buy" or not order.get("intent_id"):
        return
    template = {
        "intent_id": order["intent_id"], "code": order["code"],
        "signal_date": order["signal_date"], "registered_at": order["created_at"],
        "quote_snapshot_id": order["quote_snapshot_id"],
        "rule_input": copy.deepcopy(order.get("rule_input")),
        "rule_version": order["rule_version"],
        "rule_parameters_sha256": order["rule_parameters_sha256"],
        "rule_parameters": copy.deepcopy(order["rule_parameters"]),
        "cost_rules": dict(order["cost_rules"]), "shares": order["shares"],
        "observed_close_path": [],
        "execution_policy": copy.deepcopy(order.get("execution_policy")),
        "status": "pending_fill", "reason": "等待登记后的下一完整日线开盘核对",
        "entry_date": None, "entry_price": None, "entry_commission": None,
        "sessions_observed": 0, "observation_date": None,
        "observed_net_pct": None, "observation_snapshot_id": None,
        "data_source": order.get("data_source"),
        "source_retrieved_at": order.get("source_retrieved_at"),
    }
    for sessions, key in ((10, "ten_day_observations"), (5, "five_day_observations")):
        item = copy.deepcopy(template)
        item["observation_policy"] = _observation_policy(sessions)
        d.setdefault(key, []).append(item)


def _finish_observation(d, order, status, reason, observation_date=None):
    for item in _order_observations(d, order):
        if item["status"] in ("pending_fill", "observing"):
            item.update({"status": status, "reason": reason,
                         "observation_date": observation_date})


def _estimated_net_pct(item, close):
    """Close-based liquidation estimate with sell costs; no same-close fill."""
    shares = int(item["shares"])
    rules = item["cost_rules"]
    slip = float(rules["slippage_pct"]) / 100
    sell_price = math.floor(float(close) * (1 - slip) * 100 + 1e-9) / 100
    amount = round(sell_price * shares, 2)
    commission, stamp = _costs(amount, "sell", rules)
    paid = float(item["entry_price"]) * shares + float(item["entry_commission"])
    if paid <= 0 or sell_price <= 0:
        return None
    return round((amount - commission - stamp) / paid * 100 - 100, 2)


def _advance_observations(d, today):
    evidence_cache = {}
    for horizon, item in _observation_items(d):
        if item.get("status") != "observing":
            continue
        try:
            entry_day = datetime.strptime(item["entry_date"], "%Y-%m-%d").date()
            key = (item["code"], item["entry_date"])
            if key not in evidence_cache:
                try:
                    evidence_cache[key] = _evidence(item["code"], today, item["entry_date"])
                except Exception as error:
                    evidence_cache[key] = error
            if isinstance(evidence_cache[key], Exception):
                raise evidence_cache[key]
            frame, snapshot = evidence_cache[key]
        except Exception:
            item["reason"] = "观察行情暂时读取失败；仍待核对"
            continue
        stale_but_recoverable = (frame is not None and snapshot.get("status") == "stale"
                                 and not snapshot.get("blocking_issues"))
        if frame is None or (not snapshot["current_signal_eligible"] and not stale_but_recoverable):
            item.update({"status": "data_insufficient",
                         "reason": signal_snapshot.unavailable_reason(snapshot)})
            continue
        if any("历史区间剔除" in issue for issue in snapshot.get("quality_issues", [])):
            item.update({"status": "data_insufficient", "reason": "观察区间存在被清洗剔除的日线，无法确认连续路径"})
            continue
        policy = item.get("observation_policy") or {}
        if policy != _observation_policy(horizon):
            item.update({"status": "data_insufficient", "reason": "观察口径缺失或与当前版本不一致"})
            continue
        future = frame.loc[frame["date"].dt.date.gt(entry_day)].head(policy["sessions"])
        previous_path = item.get("observed_close_path", [])
        current_path = [{"date": bar["date"].date().isoformat(),
                         "close": round(float(bar["close"]), 6)}
                        for _, bar in future.iterrows()]
        if current_path[:len(previous_path)] != previous_path:
            item.update({"status": "data_insufficient", "reason": "已观察日线被修订或缺失，路径不可追溯"})
            continue
        if not current_path and (today - entry_day).days > policy["max_gap_calendar_days"]:
            item.update({"status": "data_insufficient", "reason": "买入后超过14个自然日仍无观察日线"})
            continue
        previous = entry_day
        for index, (_, bar) in enumerate(future.iterrows(), start=1):
            bar_day = bar["date"].date()
            if (bar_day - previous).days > policy["max_gap_calendar_days"]:
                item.update({"status": "data_insufficient", "reason": "观察日线间隔超过14个自然日",
                             "observation_date": bar_day.isoformat()})
                break
            previous = bar_day
            issues = signal_snapshot.latest_bar_quality(bar)
            if issues:
                item.update({"status": "data_insufficient", "reason": "；".join(issues),
                             "observation_date": bar_day.isoformat()})
                break
            net = _estimated_net_pct(item, bar["close"])
            if net is None:
                item.update({"status": "data_insufficient", "reason": "费用后估值无法计算",
                             "observation_date": bar_day.isoformat()})
                break
            item.update({"sessions_observed": index, "observation_date": bar_day.isoformat(),
                         "observed_net_pct": net,
                         "observed_close_path": current_path[:index],
                         "data_source": snapshot["source"],
                         "source_retrieved_at": snapshot["retrieved_at"]})
            if net >= policy["target_net_pct"]:
                outcome = "target_observed"
            elif net <= policy["risk_net_pct"]:
                outcome = "risk_observed"
            elif index == policy["sessions"]:
                outcome = "timeout_observed"
            else:
                continue
            checked_frame = frame.loc[frame["date"].dt.date.le(bar_day)].copy()
            checked_frame.attrs.update(frame.attrs)
            item.update({"status": outcome,
                         "reason": "按完整收盘价扣假设卖出费用的观察结局；未按该收盘价实际卖出",
                         "revision_audit_status": "baseline",
                         "revision_audited_at": datetime.now(entry_signals.SHANGHAI).isoformat(timespec="seconds"),
                         "revision_audit_reason": "结局已冻结；等待后续行情修订复核",
                         "observation_snapshot_id": signal_snapshot.quote_snapshot_id(
                             item["code"], checked_frame, bar_day + timedelta(days=1))})
            break
        if (item["status"] == "observing" and current_path
                and (today - datetime.strptime(current_path[-1]["date"], "%Y-%m-%d").date()).days
                > policy["max_gap_calendar_days"]):
            item.update({"status": "data_insufficient", "reason": "观察日线超过14个自然日未更新"})


def _audit_terminal_observations(d, today, checked_at):
    """Recheck a bounded number of frozen outcomes without changing their class."""
    def unavailable(item, reason):
        if item.get("revision_audit_status") == "revised":
            item["revision_audit_reason"] = "曾发现原路径修订；" + reason
        else:
            item.update({"revision_audit_status": "unavailable",
                         "revision_audit_reason": reason})

    audited = 0
    terminal = {"target_observed", "risk_observed", "timeout_observed"}
    for _, item in _observation_items(d):
        if audited >= REVISION_AUDIT_MAX_PER_CHECK:
            break
        if item.get("status") not in terminal:
            continue
        prior_at = item.get("revision_audited_at")
        try:
            prior_day = datetime.fromisoformat(prior_at).date() if prior_at else None
        except (TypeError, ValueError):
            prior_day = None
        if prior_day and (today - prior_day).days < REVISION_AUDIT_INTERVAL_DAYS:
            continue
        audited += 1
        path = item.get("observed_close_path")
        item["revision_audited_at"] = checked_at
        if not isinstance(path, list) or not path:
            item.update({"revision_audit_status": "missing_basis",
                         "revision_audit_reason": "原结局缺已观察收盘路径，不能事后补造"})
            continue
        try:
            entry_day = datetime.strptime(item["entry_date"], "%Y-%m-%d").date()
            original_days = [datetime.strptime(point["date"], "%Y-%m-%d").date()
                             for point in path]
            if (original_days != sorted(set(original_days))
                    or any(day <= entry_day for day in original_days)
                    or any(not math.isfinite(float(point["close"])) for point in path)):
                raise ValueError("invalid original observation path")
        except (KeyError, TypeError, ValueError, OverflowError):
            item.update({"revision_audit_status": "missing_basis",
                         "revision_audit_reason": "原结局观察路径日期或收盘值无效，不能事后补造"})
            continue
        try:
            frame, snapshot = _evidence(item["code"], today, item["entry_date"])
        except Exception:
            unavailable(item, "复核行情暂时读取失败；原结局未改写")
            continue
        usable_stale = (frame is not None and snapshot.get("status") == "stale"
                        and not snapshot.get("blocking_issues"))
        if (frame is None or (not snapshot["current_signal_eligible"] and not usable_stale)
                or any("历史区间剔除" in issue for issue in snapshot.get("quality_issues", []))):
            unavailable(item, "复核行情来源、时间或质量不足；原结局未改写")
            continue
        if frame.empty or frame["date"].iloc[0].date() > entry_day:
            unavailable(item, "复核行情未覆盖原入场日；无法判定历史修订")
            continue
        future = frame.loc[(frame["date"].dt.date.gt(entry_day))
                           & (frame["date"].dt.date.le(original_days[-1]))]
        current_days = {day.date() for day in future["date"]}
        if not set(original_days).issubset(current_days):
            unavailable(item, "复核行情缺原观察日线；无法区分缺数和历史修订")
            continue
        current_path = [{"date": bar["date"].date().isoformat(),
                         "close": round(float(bar["close"]), 6)}
                        for _, bar in future.iterrows()]
        item["revision_audit_source"] = snapshot.get("source")
        item["revision_audit_retrieved_at"] = snapshot.get("retrieved_at")
        if item.get("revision_audit_status") == "revised" or current_path != path:
            item.update({"revision_audit_status": "revised",
                         "revision_audit_reason": "已发现原观察收盘路径与当前行情不同；原结局保留待人工复核"})
        elif snapshot.get("source") != item.get("data_source"):
            item.update({"revision_audit_status": "source_changed",
                         "revision_audit_reason": "当前来源与结局来源不同；收盘路径相同，仍需留意口径"})
        else:
            item.update({"revision_audit_status": "consistent",
                         "revision_audit_reason": "本次复核的已观察收盘路径与原记录一致"})


def _event(d, order, status, reason, checked_at, execution_date=None):
    if status in ("blocked", "quality_rejected") and any(e.get("code") == order["code"]
                                   and e.get("status") == status
                                   and e.get("signal_date") == order["signal_date"]
                                   and e.get("reason") == reason
                                   for e in d["order_events"][-100:]):
        return
    d["order_events"].append({"code": order["code"], "name": order.get("name") or order["code"],
                              "intent_id": order.get("intent_id"),
                              "rule_version": order.get("rule_version"),
                              "rule_parameters_sha256": order.get("rule_parameters_sha256"),
                              "quote_snapshot_id": order.get("quote_snapshot_id"),
                              "rule_input": copy.deepcopy(order.get("rule_input")),
                              "action": order["action"], "signal_date": order["signal_date"],
                              "status": status, "reason": reason, "checked_at": checked_at,
                              "execution_date": execution_date})
    if status in ("unfilled", "blocked"):
        _finish_observation(d, order, "unfilled", reason, execution_date)
    elif status == "data_insufficient":
        _finish_observation(d, order, "data_insufficient", reason, execution_date)


def _valid_pending_order(order):
    try:
        if order.get("action") not in ("buy", "sell") or not order.get("intent_id"):
            return False
        if trade_constraints.quantity_error(order.get("code"), order["action"], order["shares"]):
            return False
        current_policy = trade_constraints.profile(order["code"])
        if order.get("execution_policy") is not None and order["execution_policy"] != current_policy:
            return False
        rules = order["cost_rules"]
        fee = float(rules["broker_fee_pct"])
        slip = float(rules["slippage_pct"])
        minimum = float(rules["min_commission"])
        if not all(math.isfinite(value) for value in (fee, slip, minimum)):
            return False
        if fee < 0 or not 0 <= slip < 100 or minimum < 0:
            return False
        if order["action"] == "buy" and not math.isfinite(float(order["budget_cap"])):
            return False
        return True
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def _resolve_pending(d, today, checked_at, log):
    remaining = []
    for order in d["pending_orders"]:
        if (not rule_contract.matches(order) or not order.get("quote_snapshot_id")
                or not _valid_pending_order(order)):
            reason = trade_constraints.quantity_error(order.get("code"), order.get("action"), order.get("shares"))
            _event(d, order, "data_insufficient", reason or "待订单的规则/报价/费用/股数或数量规则证据不合格，不能补记成交", checked_at)
            log.append(f"证据不足 {order['code']}：旧待订单未补记成交")
            continue
        try:
            signal_day = datetime.strptime(order["signal_date"], "%Y-%m-%d").date()
            created_day = datetime.strptime(str(order.get("created_at") or "")[:10], "%Y-%m-%d").date()
        except ValueError:
            _event(d, order, "data_insufficient", "信号或登记日期无效", checked_at)
            continue
        if signal_day >= created_day:
            _event(d, order, "data_insufficient", "信号日未早于登记日，不能补记成交", checked_at)
            continue
        earliest = max(order["signal_date"], created_day.isoformat())
        try:
            frame, snapshot = _evidence(order["code"], today, order.get("signal_date"),
                                        verify_secondary=True)
        except Exception:
            frame, snapshot = None, None
        if frame is None:
            if (today - datetime.strptime(earliest, "%Y-%m-%d").date()).days > 14:
                _event(d, order, "data_insufficient", "超过14天仍无可核对日线，无法判定是否可成交", checked_at)
                log.append(f"证据不足 {order['code']}：信号过期")
            else:
                remaining.append(order)
            continue
        signal_snapshot.require_independent_source(snapshot)
        if not snapshot["current_signal_eligible"]:
            _event(d, order, "data_insufficient",
                   signal_snapshot.unavailable_reason(snapshot), checked_at)
            log.append(f"证据不足 {order['code']}：待成交行情不合格")
            continue
        # Registration must predate the assumed opening fill; never backfill a day already over.
        future = frame.loc[frame["date"].dt.strftime("%Y-%m-%d") > earliest]
        if future.empty:
            if (today - datetime.strptime(earliest, "%Y-%m-%d").date()).days > 14:
                _event(d, order, "data_insufficient", "超过14天无下一条完整日线，无法判定是否可成交", checked_at)
                log.append(f"证据不足 {order['code']}：信号过期")
            else:
                remaining.append(order)
            continue
        bar = future.iloc[0]
        fill_date = bar["date"].strftime("%Y-%m-%d")
        latest_date = frame["date"].iloc[-1].strftime("%Y-%m-%d")
        reason = None
        execution_policy = trade_constraints.profile(order["code"])
        if fill_date < execution_policy["effective_from"]:
            reason = "执行日期早于本版已核对交易规则的生效范围，不回填当前规则"
        elif fill_date != latest_date:
            reason = "错过下一完整交易日的检查，未追溯补记成交"
        elif (bar["date"].date() - datetime.strptime(earliest, "%Y-%m-%d").date()).days > 14:
            reason = "登记后超过14天才出现日线，可能停牌或缺数据"
        elif any(not math.isfinite(float(bar[k])) or float(bar[k]) <= 0 for k in ("open", "high", "low", "volume")):
            reason = "开盘价/最高最低价/成交量无效或零成交"
        elif float(bar["high"]) == float(bar["low"]):
            reason = "全天单一价格，无法确认排队可成交"
        elif sum(1 for t in d["trades"] if t.get("date") == fill_date) >= d["rules"]["max_trades_per_day"]:
            reason = "当日模拟成交次数已达上限"
        position = next((p for p in d["positions"] if p["code"] == order["code"]), None)
        if not reason and order["action"] == "sell":
            acquired = _legacy_acquired(position, d["trades"]) if position else None
            if position is None:
                reason = "持仓已不存在"
            elif not acquired:
                reason = "买入日期缺失，无法核实T+1"
            elif acquired >= fill_date:
                reason = "T+1：买入日及之前不可卖"
            else:
                reason = trade_constraints.quantity_error(
                    order["code"], "sell", order["shares"], position["shares"])
        if not reason and order["action"] == "buy" and position:
            reason = "已持有该股票"
        if reason:
            _event(d, order, "unfilled", reason, checked_at, fill_date)
            log.append(f"未成交 {order['code']}：{reason}")
            continue
        base = float(bar["open"])
        cost_rules = order["cost_rules"]
        slip = float(cost_rules["slippage_pct"]) / 100
        raw_price = base * (1 + slip if order["action"] == "buy" else 1 - slip)
        price = (math.ceil(raw_price * 100 - 1e-9) if order["action"] == "buy"
                 else math.floor(raw_price * 100 + 1e-9)) / 100
        if price <= 0 or price > float(bar["high"]) + 0.001 or price < float(bar["low"]) - 0.001:
            reason = "滑点后价格超出当日价格范围，无法按该假设成交"
            _event(d, order, "unfilled", reason, checked_at, fill_date)
            log.append(f"未成交 {order['code']}：{reason}")
            continue
        shares = trade_constraints.integer_shares(order["shares"])
        amount = round(price * shares, 2)
        commission, stamp = _costs(amount, order["action"], cost_rules)
        if order["action"] == "buy":
            if amount + commission > d["cash"] + 0.001:
                reason = "含佣金资金不足"
            elif amount + commission > float(order["budget_cap"]) + 0.001:
                reason = "开盘价和费用超过信号日仓位预算"
            if reason:
                _event(d, order, "unfilled", reason, checked_at, fill_date)
                log.append(f"未成交 {order['code']}：{reason}")
                continue
            d["cash"] = round(d["cash"] - amount - commission, 2)
            d["positions"].append({"code": order["code"], "name": order.get("name") or order["code"],
                                   "shares": shares, "avg_cost": round((amount + commission) / shares, 3),
                                   "acquired_on": fill_date})
        else:
            if shares <= 0 or d["cash"] + amount < commission + stamp:
                reason = "卖出数量无效或可用资金不足以支付费用"
                _event(d, order, "unfilled", reason, checked_at, fill_date)
                log.append(f"未成交 {order['code']}：{reason}")
                continue
            d["cash"] = round(d["cash"] + amount - commission - stamp, 2)
            d["positions"] = [p for p in d["positions"] if p["code"] != order["code"]]
        d["trades"].append({"date": fill_date, "time": "", "code": order["code"],
                            "intent_id": order.get("intent_id"),
                            "rule_version": order["rule_version"],
                            "rule_parameters_sha256": order["rule_parameters_sha256"],
                            "signal_quote_snapshot_id": order["quote_snapshot_id"],
                            "signal_rule_input": copy.deepcopy(order.get("rule_input")),
                            "fill_quote_snapshot_id": snapshot["quote_snapshot_id"],
                            "fill_cross_source_status": snapshot["cross_source_status"],
                            "execution_policy": execution_policy,
                            "execution_policy_adopted_at_fill": order.get("execution_policy") is None,
                            "name": order.get("name") or order["code"], "action": order["action"],
                            "price": price, "open_price": round(base, 3), "shares": shares,
                            "amount": amount, "commission": commission, "stamp_tax": stamp,
                            "slippage_pct": cost_rules["slippage_pct"],
                            "signal_date": order["signal_date"], "checked_at": checked_at,
                            "price_source": frame.attrs.get("source"),
                            "price_retrieved_at": frame.attrs.get("source_retrieved_at"),
                            "fill_basis": "登记后下一完整日线开盘价推算",
                            "reason": order["reason"]})
        if order["action"] == "buy":
            for observed in _order_observations(d, order):
                observed.update({"status": "observing", "reason": "等待买入日之后的完整收盘",
                                 "entry_date": fill_date, "entry_price": price,
                                 "entry_commission": commission,
                                 "fill_quote_snapshot_id": snapshot["quote_snapshot_id"]})
        _event(d, order, "hypothetical_fill", "下一完整日线开盘价加滑点推算；未验证盘口排队/公司行动", checked_at, fill_date)
        log.append(f"虚拟成交 {order['code']} {order['action']} {shares}股 @{price}")
    d["pending_orders"] = remaining


def _check(progress=None):
    d = load()
    today = datetime.now(entry_signals.SHANGHAI).date()
    checked_at = datetime.now(entry_signals.SHANGHAI).isoformat(timespec="seconds")
    log = []
    _resolve_pending(d, today, checked_at, log)
    _advance_observations(d, today)
    _audit_terminal_observations(d, today, checked_at)
    pending_codes = {o["code"] for o in d["pending_orders"]}
    dealt_codes = {e["code"] for e in d["order_events"] if e.get("checked_at") == checked_at}
    registered_entry_signals = {(item.get("code"), item.get("signal_date"))
                                for item in d["ten_day_observations"]}

    for p in list(d["positions"]):
        code = p["code"]
        if code in pending_codes or code in dealt_codes:
            continue
        if trade_constraints.profile(code) is None:
            _event(d, {"code": code, "name": p.get("name"), "action": "sell", "signal_date": today.isoformat()},
                   "blocked", trade_constraints.quantity_error(code, "sell", p.get("shares")), checked_at)
            continue
        acquired = _legacy_acquired(p, d["trades"])
        if not acquired:
            _event(d, {"code": code, "name": p.get("name"), "action": "sell", "signal_date": today.isoformat()},
                   "blocked", "旧持仓缺买入日期，无法核实T+1及移动止盈", checked_at)
            continue
        try:
            frame, snapshot, raw, rejected = _evidence(code, today, acquired, with_raw=True)
            signal = exit_signals.evaluate_exit_frame(frame,
                {"code": code, "cost_price": p["avg_cost"], "acquired_on": acquired}, today=today)
        except Exception:
            _event(d, {"code": code, "name": p.get("name"), "action": "sell",
                       "signal_date": today.isoformat()}, "data_insufficient",
                   "持仓退出行情或规则读取失败，未登记待成交订单", checked_at)
            continue
        if signal["status"] != "risk_alert" or signal.get("execution_warning") or not signal["as_of"]:
            continue
        if snapshot["current_signal_eligible"]:
            raw = source_check.with_secondary_check(code, raw, today)
            snapshot = signal_snapshot.data_snapshot(code, raw, frame, rejected, today,
                                                     entry_signals.MAX_QUOTE_AGE_DAYS)
        signal_snapshot.require_rule_dates(snapshot, (signal, "risk_alert"))
        trace = signal_snapshot.signal_trace(signal, snapshot, "risk_alert")
        if (not trace["signal_date"] or not trace["quote_snapshot_id"]
                or signal["as_of"] >= today.isoformat()):
            _event(d, {"code": code, "name": p.get("name"), "action": "sell",
                       "signal_date": signal["as_of"]}, "quality_rejected",
                   signal_snapshot.unavailable_reason(snapshot), checked_at)
            continue
        triggers = [signal[key]["reason"] for key in ("ma21_break", "trailing_profit") if signal[key]["triggered"]]
        input_evidence = rule_inputs.evidence(code, frame, today,
            {"cost_price": p["avg_cost"], "acquired_on": acquired}, persist=True,
            storage_root=os.path.join(os.path.dirname(FILE), "rule_inputs"))
        order = {"code": code, "name": p.get("name") or code, "action": "sell",
                 "rule_input": input_evidence,
                 **rule_contract.metadata(), "quote_snapshot_id": trace["quote_snapshot_id"],
                 "intent_id": uuid.uuid4().hex,
                 "execution_policy": trade_constraints.profile(code),
                 "shares": p["shares"], "signal_date": signal["as_of"],
                 "created_at": checked_at, "reason": "；".join(triggers),
                 "cost_rules": {k: d["rules"][k] for k in ("broker_fee_pct", "slippage_pct", "min_commission")},
                 "data_source": frame.attrs.get("source") if frame is not None else None,
                 "source_retrieved_at": frame.attrs.get("source_retrieved_at") if frame is not None else None}
        if any(e.get("code") == code and e.get("action") == "sell"
               and e.get("status") in ("unfilled", "hypothetical_fill")
               and e.get("signal_date") == signal["as_of"] for e in d["order_events"]):
            continue
        d["pending_orders"].append(order)
        pending_codes.add(code)
        log.append(f"退出信号 {code}：等下一完整交易日验证虚拟成交")

    slots = min(d["rules"]["buy_top_n"],
                max(0, d["rules"]["max_trades_per_day"] - len(d["pending_orders"])))
    if slots > 0:
        if progress:
            progress("scan", 0, len(screener.UNIVERSE), "读取候选排序并核对三项入场条件")
        rows = _universe_scores(progress=progress)
        if not rows:
            log.append("当前没有日期早于今天的合格排序缓存；本轮未产生入场信号")
        inspected = 0
        for row in rows:
            if slots <= 0 or inspected >= 30:
                break
            code = row["code"]
            if code in pending_codes or code in dealt_codes or any(p["code"] == code for p in d["positions"]):
                continue
            if row.get("score", 0) < d["rules"]["buy_min_score"]:
                continue
            inspected += 1
            execution_policy = trade_constraints.profile(code)
            if execution_policy is None:
                _event(d, {"code": code, "name": row.get("name"), "action": "buy",
                           "signal_date": row.get("as_of")}, "blocked",
                       trade_constraints.quantity_error(code, "buy", 100), checked_at)
                continue
            try:
                frame, snapshot, raw, rejected = _evidence(code, today, with_raw=True)
                signal = entry_signals.evaluate_entry_frame(frame, today=today)
            except Exception:
                _event(d, {"code": code, "name": row.get("name"), "action": "buy",
                           "signal_date": row.get("as_of")}, "data_insufficient",
                       "候选行情或规则读取失败，未登记待成交订单", checked_at)
                continue
            if signal["status"] != "candidate" or frame is None:
                continue
            if snapshot["current_signal_eligible"]:
                raw = source_check.with_secondary_check(code, raw, today)
                snapshot = signal_snapshot.data_snapshot(code, raw, frame, rejected, today,
                                                         entry_signals.MAX_QUOTE_AGE_DAYS)
            signal_snapshot.require_rule_dates(snapshot, (signal, "candidate"))
            trace = signal_snapshot.signal_trace(signal, snapshot, "candidate")
            if (not trace["signal_date"] or not trace["quote_snapshot_id"]
                    or signal["as_of"] >= today.isoformat()):
                _event(d, {"code": code, "name": row.get("name"), "action": "buy",
                           "signal_date": signal["as_of"]}, "quality_rejected",
                       signal_snapshot.unavailable_reason(snapshot), checked_at)
                continue
            if row["as_of"] > signal["as_of"]:
                continue
            if (code, signal["as_of"]) in registered_entry_signals:
                continue
            close = float(frame["close"].iloc[-1])
            cost_basis = sum(float(p["avg_cost"]) * p["shares"] for p in d["positions"])
            budget = min(d["cash"], (d["cash"] + cost_basis) * d["rules"]["max_position_pct"] / 100)
            estimated_price = math.ceil(close * (1 + d["rules"]["slippage_pct"] / 100) * 100 - 1e-9) / 100
            shares = trade_constraints.affordable_shares(
                code, budget, estimated_price, d["rules"]["broker_fee_pct"], d["rules"]["min_commission"])
            input_evidence = rule_inputs.evidence(code, frame, today, persist=True,
                storage_root=os.path.join(os.path.dirname(FILE), "rule_inputs"))
            order = {"code": code, "name": row.get("name") or code, "action": "buy",
                     "rule_input": input_evidence,
                     **rule_contract.metadata(), "quote_snapshot_id": trace["quote_snapshot_id"],
                     "intent_id": uuid.uuid4().hex,
                     "execution_policy": execution_policy,
                     "shares": shares, "budget_cap": round(budget, 2), "signal_date": signal["as_of"],
                     "cost_rules": {k: d["rules"][k] for k in ("broker_fee_pct", "slippage_pct", "min_commission")},
                     "created_at": checked_at, "data_source": frame.attrs.get("source"),
                     "source_retrieved_at": frame.attrs.get("source_retrieved_at"),
                     "reason": f"候选排序第{row.get('rank')}名，技术分{row.get('score')}；13周趋势、回踩MACD、量价三项同时满足"}
            _register_observation(d, order)
            registered_entry_signals.add((code, signal["as_of"]))
            if shares < execution_policy["minimum_buy_shares"]:
                _event(d, order, "blocked", "仓位预算不足以支付该板块最低买入数量及假设佣金", checked_at)
                continue
            d["pending_orders"].append(order)
            pending_codes.add(code)
            slots -= 1
            log.append(f"入场信号 {code}：等下一完整交易日验证虚拟成交")
        if progress:
            progress("signal", inspected, min(30, len(rows)), f"已核对前{inspected}只达标排序候选")
    save(d)
    return {"summary": summary(d), "log": log}


def check(progress=None):
    if not _CHECK_LOCK.acquire(blocking=False):
        raise RuntimeError("模拟账本正在执行另一轮检查")
    try:
        return _check(progress=progress)
    finally:
        _CHECK_LOCK.release()
