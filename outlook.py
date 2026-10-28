# -*- coding: utf-8 -*-
"""持仓 5/10 日历史情景参考：本地计算、后台取数、结果带样本量和验证指标。"""
import concurrent.futures
import json
import os
import re
import threading
from datetime import datetime

import pandas as pd
import stock_agent as sa

BASE = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE, "forecast_cache")
os.makedirs(CACHE_DIR, exist_ok=True)
_LOCK = threading.Lock()
_STATE = {"running": False, "codes": [], "done": 0, "total": 0,
          "target_pct": 10.0, "stop_pct": 5.0,
          "results": {}, "errors": {}, "started_at": None, "finished_at": None}


def _cache_path(code):
    return os.path.join(CACHE_DIR, f"{code}.json")


def _cached(code, target_pct, stop_pct):
    try:
        with open(_cache_path(code), encoding="utf-8") as f:
            entry = json.load(f)
        result = entry.get("result") or {}
        scenario = result.get("target_scenario") or {}
        if (entry.get("generated_date") == datetime.now().strftime("%Y-%m-%d")
                and scenario.get("target_pct") == target_pct
                and scenario.get("stop_pct") == stop_pct):
            return result
    except Exception:
        pass
    return None


def _save(code, result):
    path = _cache_path(code)
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"generated_date": datetime.now().strftime("%Y-%m-%d"),
                       "result": result}, f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass


def _distribution(returns, states, current_state):
    values = returns[states == current_state].dropna()
    matched = len(values)
    condition = "历史中同样处于20日均线上方" if current_state else "历史中同样处于20日均线下方"
    if matched < 30:
        values = returns.dropna()
        condition = "同类样本不足30，改用全部历史样本"
    if len(values) < 20:
        return None
    return {
        "sample_size": int(len(values)),
        "matched_sample_size": int(matched),
        "condition": condition,
        "positive_probability_pct": round(float((values > 0).mean() * 100), 1),
        "median_return_pct": round(float(values.median() * 100), 2),
        "p10_return_pct": round(float(values.quantile(0.10) * 100), 2),
        "p90_return_pct": round(float(values.quantile(0.90) * 100), 2),
    }


def _walk_forward(close, states, horizon):
    """逐日滚动验证；每次预测只使用当时已走完 horizon 的历史结果。"""
    forward = close.shift(-horizon) / close - 1
    n = len(close)
    test_start = max(100, int(n * 0.70))
    test_stop = n - horizon
    brier, baseline_brier, directional, correct = [], [], [], []
    for i in range(test_start, test_stop):
        known_end = i - horizon
        if known_end < 60:
            continue
        first = max(0, known_end - 252)
        train_returns = forward.iloc[first:known_end + 1]
        train_states = states.iloc[first:known_end + 1]
        known = train_returns.notna()
        matched = train_returns[known & (train_states == states.iloc[i])]
        if len(matched) >= 30:
            sample = matched
        else:
            sample = train_returns[known]
        if len(sample) < 30 or pd.isna(forward.iloc[i]):
            continue
        probability = float((sample > 0).mean())
        baseline_probability = float((train_returns[known] > 0).mean())
        actual_up = bool(forward.iloc[i] > 0)
        brier.append((probability - int(actual_up)) ** 2)
        baseline_brier.append((baseline_probability - int(actual_up)) ** 2)
        if probability >= 0.55:
            directional.append(actual_up)
            correct.append(actual_up)
        elif probability <= 0.45:
            directional.append(not actual_up)
            correct.append(not actual_up)
    mean_brier = sum(brier) / len(brier) if brier else None
    mean_baseline = sum(baseline_brier) / len(baseline_brier) if baseline_brier else None
    return {
        "test_observations": len(brier),
        "directional_calls": len(correct),
        "directional_accuracy_pct": round(sum(correct) / len(correct) * 100, 1) if correct else None,
        "brier_score": round(mean_brier, 3) if mean_brier is not None else None,
        "baseline_brier_score": round(mean_baseline, 3) if mean_baseline is not None else None,
        "brier_skill_pct": round((1 - mean_brier / mean_baseline) * 100, 1)
        if mean_brier is not None and mean_baseline and mean_baseline > 0 else None,
        "method": "最近30%历史的逐日滚动验证；训练样本只使用预测日之前已完整走完的区间",
    }


def _advice(price, ma20, forecasts, validation):
    f5, f10 = forecasts.get("5"), forecasts.get("10")
    if not f5 or not f10:
        return {"label": "数据不足", "action": "先补充足够的日线历史数据，再查看趋势参考。"}
    v10 = validation.get("10") or {}
    accuracy = v10.get("directional_accuracy_pct")
    calls = v10.get("directional_calls", 0)
    skill = v10.get("brier_skill_pct")
    reliable = v10.get("test_observations", 0) >= 30 and (
        (skill is not None and skill >= 5) or (calls >= 30 and accuracy is not None and accuracy >= 55)
    )
    if not reliable:
        return {"label": "趋势信号待验证", "action": "5日和10日历史统计在滚动检验中没有显示稳定优势。把它当作行情背景；是否执行请看上方目标情景、风险线和自己的计划。"}
    p5, p10 = f5["positive_probability_pct"], f10["positive_probability_pct"]
    median10 = f10["median_return_pct"]
    above = price >= ma20 if ma20 else None
    if above and p5 >= 55 and p10 >= 55 and median10 > 0:
        return {"label": "历史趋势偏强", "action": "当前均线状态下，历史正收益频率和中位数相对偏强；这不是未来上涨概率，也不等于适合买入。"}
    if above is False and p10 < 45 and median10 < 0:
        return {"label": "历史趋势偏弱", "action": "当前均线状态下，历史10日样本偏弱；这只能提示风险背景，不能单独决定买入或卖出。"}
    return {"label": "趋势信号不一致", "action": "5日与10日历史统计没有形成一致方向；查看上方情景样本与示例条件，不要把单一指标当成交易结论。"}


def _scenario_events(d, target_pct, stop_pct, horizon):
    """对每个历史收盘入场点，检查随后 horizon 根日线中哪条价位先被触及。"""
    close = d["close"].astype(float).reset_index(drop=True)
    high = pd.to_numeric(d["high"], errors="coerce").reset_index(drop=True)
    low = pd.to_numeric(d["low"], errors="coerce").reset_index(drop=True)
    events = [None] * len(d)
    ambiguous = [False] * len(d)
    target = target_pct / 100
    stop = stop_pct / 100
    for i in range(max(0, len(d) - horizon)):
        entry = close.iloc[i]
        if pd.isna(entry) or entry <= 0:
            continue
        outcome = "timeout"
        complete_path = True
        for j in range(i + 1, i + horizon + 1):
            if pd.isna(high.iloc[j]) or pd.isna(low.iloc[j]):
                complete_path = False
                break
            hit_target = high.iloc[j] >= entry * (1 + target)
            hit_stop = low.iloc[j] <= entry * (1 - stop)
            if hit_target and hit_stop:
                # 日线无法还原先后顺序，同日双触及一律按风险线先到处理。
                outcome, ambiguous[i] = "stop", True
                break
            if hit_stop:
                outcome = "stop"
                break
            if hit_target:
                outcome = "target"
                break
        if complete_path:
            events[i] = outcome
    return events, ambiguous


def _scenario_summary(events, ambiguous, states, current_state, target_pct, stop_pct, horizon):
    known = [i for i, event in enumerate(events) if event is not None]
    matched = [i for i in known if bool(states.iloc[i]) == bool(current_state)]
    indices = matched
    condition = "历史中同样处于20日均线上方" if current_state else "历史中同样处于20日均线下方"
    if len(indices) < 30:
        indices = known
        condition = "同类样本不足30，改用全部历史样本"
    n = len(indices)
    if n < 20:
        return {"horizon": horizon, "target_pct": target_pct, "stop_pct": stop_pct,
                "sample_size": n, "matched_sample_size": len(matched), "condition": condition,
                "target_first_pct": None, "stop_first_pct": None, "timeout_pct": None,
                "target_first_count": 0, "stop_first_count": 0, "timeout_count": 0,
                "ambiguous_same_day": sum(ambiguous[i] for i in indices)}
    counts = {"target": 0, "stop": 0, "timeout": 0}
    for i in indices:
        counts[events[i]] += 1
    return {
        "horizon": horizon,
        "target_pct": target_pct,
        "stop_pct": stop_pct,
        "sample_size": n,
        "matched_sample_size": len(matched),
        "condition": condition,
        "target_first_pct": round(counts["target"] / n * 100, 1),
        "stop_first_pct": round(counts["stop"] / n * 100, 1),
        "timeout_pct": round(counts["timeout"] / n * 100, 1),
        "target_first_count": counts["target"],
        "stop_first_count": counts["stop"],
        "timeout_count": counts["timeout"],
        "ambiguous_same_day": sum(ambiguous[i] for i in indices),
    }


def _scenario_validation(events, states, horizon):
    """最近30%区间做滚动检验，训练标签均须在预测日之前完整结束。"""
    n = len(events)
    test_start = max(100, int(n * 0.70))
    brier, baseline_brier = [], []
    for i in range(test_start, n - horizon):
        known_end = i - horizon - 1
        if known_end < 60 or events[i] is None:
            continue
        first = max(0, known_end - 252)
        train = [j for j in range(first, known_end + 1) if events[j] is not None]
        if len(train) < 30:
            continue
        matched = [j for j in train if bool(states.iloc[j]) == bool(states.iloc[i])]
        sample = matched if len(matched) >= 30 else train
        probability = sum(events[j] == "target" for j in sample) / len(sample)
        baseline = sum(events[j] == "target" for j in train) / len(train)
        actual = int(events[i] == "target")
        brier.append((probability - actual) ** 2)
        baseline_brier.append((baseline - actual) ** 2)
    mean_brier = sum(brier) / len(brier) if brier else None
    mean_baseline = sum(baseline_brier) / len(baseline_brier) if baseline_brier else None
    return {
        "test_observations": len(brier),
        "brier_score": round(mean_brier, 3) if mean_brier is not None else None,
        "baseline_brier_score": round(mean_baseline, 3) if mean_baseline is not None else None,
        "brier_skill_pct": round((1 - mean_brier / mean_baseline) * 100, 1)
        if mean_brier is not None and mean_baseline and mean_baseline > 0 else None,
        "method": "最近30%历史逐日滚动检验；只使用预测日之前已完整走完的历史区间，测试窗口彼此重叠",
    }


def _entry_check(close, ma20, rsi14):
    momentum5 = float(close.iloc[-1] / close.iloc[-6] - 1) if len(close) >= 6 else None
    conditions = [
        {"label": "收盘价在20日均线上方", "passed": bool(close.iloc[-1] >= ma20.iloc[-1]) if pd.notna(ma20.iloc[-1]) else False},
        {"label": "近5个交易日收盘涨幅为正", "passed": momentum5 is not None and momentum5 > 0},
        {"label": "RSI14低于70（避免追入过热区）", "passed": pd.notna(rsi14.iloc[-1]) and float(rsi14.iloc[-1]) < 70},
    ]
    passed = sum(item["passed"] for item in conditions)
    return {
        "status": "通过示例观察条件" if passed == len(conditions) else "暂未通过示例观察条件",
        "passed": passed,
        "total": len(conditions),
        "conditions": conditions,
        "momentum5_pct": round(momentum5 * 100, 2) if momentum5 is not None else None,
        "rsi14": round(float(rsi14.iloc[-1]), 1) if pd.notna(rsi14.iloc[-1]) else None,
        "rule": "最近收盘价高于MA20、近5日收盘涨幅为正、RSI14低于70；这是待验证的示例过滤规则，不是买入信号。",
    }


def _build(code, target_pct=10.0, stop_pct=5.0):
    end = pd.Timestamp.today().strftime("%Y%m%d")
    start = (pd.Timestamp.today() - pd.Timedelta(days=900)).strftime("%Y%m%d")
    df = sa._kline_fast(code, start, end)
    if df is None or df.empty:
        raise ValueError("日线数据暂不可用")
    d = df.copy()
    d["close"] = pd.to_numeric(d["close"], errors="coerce")
    for column in ("high", "low"):
        if column not in d.columns:
            d[column] = d["close"]
        d[column] = pd.to_numeric(d[column], errors="coerce")
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = d.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates("date").reset_index(drop=True)
    if len(d) < 120:
        raise ValueError("有效日线不足120个交易日，无法进行历史验证")
    close = d["close"].astype(float)
    ma20 = close.rolling(20).mean()
    states = close >= ma20
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    rsi14 = 100 - 100 / (1 + gain / loss.replace(0, 1e-12))
    price = float(close.iloc[-1])
    current_ma20 = float(ma20.iloc[-1]) if pd.notna(ma20.iloc[-1]) else None
    forecasts, validation = {}, {}
    for horizon in (5, 10):
        forward = close.shift(-horizon) / close - 1
        summary = _distribution(forward, states, bool(states.iloc[-1]))
        if summary is None:
            forecasts[str(horizon)] = None
        else:
            summary.update({
                "days": horizon,
                "median_price": round(price * (1 + summary["median_return_pct"] / 100), 2),
                "range_low_price": round(price * (1 + summary["p10_return_pct"] / 100), 2),
                "range_high_price": round(price * (1 + summary["p90_return_pct"] / 100), 2),
            })
            forecasts[str(horizon)] = summary
        validation[str(horizon)] = _walk_forward(close, states, horizon)
    events, ambiguous = _scenario_events(d, target_pct, stop_pct, 10)
    target_scenario = _scenario_summary(events, ambiguous, states, bool(states.iloc[-1]),
                                        target_pct, stop_pct, 10)
    target_validation = _scenario_validation(events, states, 10)
    entry_check = _entry_check(close, ma20, rsi14)
    try:
        name = sa._name_map().get(code) or code
    except Exception:
        name = code
    return {
        "code": code,
        "name": name,
        "as_of": d["date"].iloc[-1].strftime("%Y-%m-%d"),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "price": round(price, 2),
        "ma20": round(current_ma20, 2) if current_ma20 is not None else None,
        "above_ma20": bool(current_ma20 is not None and price >= current_ma20),
        "forecasts": forecasts,
        "validation": validation,
        "target_scenario": target_scenario,
        "target_validation": target_validation,
        "entry_check": entry_check,
        "advice": _advice(price, current_ma20, forecasts, validation),
        "caveat": "这是历史回放，不是未来概率或买卖建议。历史样本以每日收盘作为假设入场点，10日窗口彼此重叠，不是独立样本。日线回放不能还原盘中先后与实际成交；同日同时触及目标和风险线按风险线先到处理。结果未计手续费、滑点、涨跌停及T+1造成的执行差异。",
    }


def _run(pending, target_pct, stop_pct):
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, len(pending))) as pool:
            futures = {pool.submit(_build, code, target_pct, stop_pct): code for code in pending}
            for future in concurrent.futures.as_completed(futures):
                code = futures[future]
                try:
                    result = future.result()
                    _save(code, result)
                    with _LOCK:
                        _STATE["results"][code] = result
                except Exception as e:
                    with _LOCK:
                        _STATE["errors"][code] = str(e)[:200]
                finally:
                    with _LOCK:
                        _STATE["done"] += 1
    finally:
        with _LOCK:
            _STATE["running"] = False
            _STATE["finished_at"] = datetime.now().isoformat(timespec="seconds")


def start(codes, force=False, target_pct=10.0, stop_pct=5.0):
    target_pct = round(float(target_pct), 2)
    stop_pct = round(float(stop_pct), 2)
    codes = list(dict.fromkeys(str(c).strip() for c in codes if re.fullmatch(r"\d{6}", str(c).strip())))
    if not codes:
        return status()
    with _LOCK:
        if _STATE["running"]:
            return json.loads(json.dumps(_STATE, ensure_ascii=False))
        today_results = {} if force else {c: _cached(c, target_pct, stop_pct) for c in codes}
        today_results = {c: r for c, r in today_results.items() if r}
        pending = [c for c in codes if c not in today_results]
        _STATE.update({"running": bool(pending), "codes": codes, "done": len(today_results),
                       "total": len(codes), "results": today_results, "errors": {},
                       "target_pct": target_pct, "stop_pct": stop_pct,
                       "started_at": datetime.now().isoformat(timespec="seconds"),
                       "finished_at": None if pending else datetime.now().isoformat(timespec="seconds")})
    if pending:
        try:
            threading.Thread(target=_run, args=(pending, target_pct, stop_pct), daemon=True).start()
        except Exception as e:
            with _LOCK:
                _STATE["running"] = False
                _STATE["errors"]["system"] = str(e)[:200]
                _STATE["finished_at"] = datetime.now().isoformat(timespec="seconds")
    return status()


def status():
    with _LOCK:
        return json.loads(json.dumps(_STATE, ensure_ascii=False))
