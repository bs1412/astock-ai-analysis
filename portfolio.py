# -*- coding: utf-8 -*-
"""组合级研究：因子池 -> IC筛选(训练期) -> 动态加权回测 -> 与固定权重模型对比 + 样本外验证。"""
import math
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np
import stock_agent as sa
import concurrent.futures

FACTOR_NAMES = ["mom_1", "mom_5", "mom_20", "mom_60", "trend", "rsi50", "macd_norm", "boll_pos", "vol_ratio", "range_pos"]
FIXED_WEIGHTS = {"trend": 0.4, "momentum": 0.4, "volatility": 0.2}
MARKET_BENCHMARK = {"name": "中证全指", "code": "000985", "symbol": "csi000985"}
PORTFOLIO_LOOKBACK_DAYS = 1095
ROLLING_FOLD_COUNT = 4
MIN_BACKTEST_DATES = 60
MIN_COMMON_DATES_FOR_ROLLING = 598
STAMP_DUTY_CHANGE_DATE = "2023-08-28"
STAMP_DUTY_RATE_BEFORE = 0.001
STAMP_DUTY_RATE_AFTER = 0.0005


def _sell_stamp_duty_rate(date):
    """Mainland A-share securities trading stamp duty, charged on sales only."""
    return (STAMP_DUTY_RATE_AFTER if str(date) >= STAMP_DUTY_CHANGE_DATE
            else STAMP_DUTY_RATE_BEFORE)


def _trade_cost_rate(buy_weight, sell_weight, date, fee_rate, slippage_rate):
    side_rate = fee_rate + slippage_rate
    return side_rate * (buy_weight + sell_weight) + _sell_stamp_duty_rate(date) * sell_weight


def _trade_sides(old_weights, new_weights):
    codes = set(old_weights) | set(new_weights)
    buy = sum(max(0.0, new_weights.get(c, 0.0) - old_weights.get(c, 0.0)) for c in codes)
    sell = sum(max(0.0, old_weights.get(c, 0.0) - new_weights.get(c, 0.0)) for c in codes)
    return buy, sell


def _marked_weights(holdings, close, i):
    """Mark position weights to the execution close before calculating turnover."""
    if not holdings or i == 0:
        return holdings, 0.0
    relatives = {c: float(close[c].iloc[i] / close[c].iloc[i - 1]) for c in holdings}
    gross = sum(holdings[c] * relatives[c] for c in holdings)
    if not math.isfinite(gross) or gross <= 0:
        raise ValueError("行情价格无效，无法计算组合持仓权重")
    return {c: holdings[c] * relatives[c] / gross for c in holdings}, gross - 1


def _normalize_trade_date(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float)) and float(value).is_integer():
        raw = str(int(value))
    else:
        raw = str(value).strip()
    parsed = (pd.to_datetime(raw, format="%Y%m%d", errors="coerce")
              if len(raw) == 8 and raw.isdigit()
              else pd.to_datetime(raw, errors="coerce"))
    return None if pd.isna(parsed) else parsed.strftime("%Y-%m-%d")


def _rsi14(s):
    d = s.diff()
    g = d.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    return 100 - 100 / (1 + g / l.replace(0, 1e-12))


def _macd_hist(s):
    e12 = s.ewm(span=12, adjust=False).mean()
    e26 = s.ewm(span=26, adjust=False).mean()
    dif = e12 - e26
    dea = dif.ewm(span=9, adjust=False).mean()
    return (dif - dea) * 2


def _fetch(pool, progress=None):
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    end = now.strftime("%Y%m%d")
    start = (now - timedelta(days=PORTFOLIO_LOOKBACK_DAYS)).strftime("%Y%m%d")
    completed_before = now.strftime("%Y-%m-%d")
    frames = {}
    lineage = {}

    def _one(code):
        try:
            return code, sa._kline_fast(code, start, end)
        except Exception:
            return code, None

    if not pool:
        return frames, lineage, completed_before
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(pool))) as executor:
        futures = [executor.submit(_one, code) for code in pool]
        for done, future in enumerate(concurrent.futures.as_completed(futures), 1):
            code, df = future.result()
            if progress:
                progress("fetch", done, len(pool), f"正在获取行情 {done}/{len(pool)}")
            if df is not None and not df.empty:
                source = df.attrs.get("source")
                retrieved_at = df.attrs.get("source_retrieved_at")
                df = df.copy()
                df["date"] = df["date"].map(_normalize_trade_date)
                for c in ("open", "close", "high", "low", "volume"):
                    df[c] = pd.to_numeric(df[c], errors="coerce")
                prices = df[["open", "close", "high", "low", "volume"]]
                finite = pd.Series(np.isfinite(prices.to_numpy(dtype=float)).all(axis=1), index=df.index)
                plausible = (prices[["open", "close", "high", "low"]].gt(0).all(axis=1)
                             & df["volume"].ge(0) & df["high"].ge(df["low"])
                             & df["open"].ge(df["low"]) & df["open"].le(df["high"])
                             & df["close"].ge(df["low"]) & df["close"].le(df["high"]))
                completed = df["date"].notna() & df["date"].fillna("").lt(completed_before)
                invalid_count = int((~(df["date"].notna() & finite & plausible)).sum())
                excluded_unfinished = int((df["date"].notna() & ~completed & finite & plausible).sum())
                df = (df.loc[completed & finite & plausible]
                        .drop_duplicates("date", keep="last")
                        .sort_values("date").reset_index(drop=True))
                lineage[code] = {"data_source": source, "source_retrieved_at": retrieved_at,
                                 "as_of": df["date"].iloc[-1] if not df.empty else None,
                                 "valid_bar_count": len(df),
                                 "rejected_invalid_bar_count": invalid_count,
                                 "excluded_unfinished_or_future_bar_count": excluded_unfinished}
                if not df.empty:
                    frames[code] = df
    return frames, lineage, completed_before


def _factor_panel(frames, horizon=10):
    parts = []
    for code, f in frames.items():
        d = f.copy()
        close, high, low, vol = d["close"], d["high"], d["low"], d["volume"]
        rsi = _rsi14(close)
        macd = _macd_hist(close)
        ma20 = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        h60, l60 = high.rolling(60).max(), low.rolling(60).min()
        df = pd.DataFrame({
            "date": d["date"], "target_date": d["date"].shift(-horizon), "code": code,
            "ret": close.shift(-horizon) / close - 1,
            "mom_1": close.pct_change(1), "mom_5": close.pct_change(5),
            "mom_20": close.pct_change(20), "mom_60": close.pct_change(60),
            "trend": close / ma20 - 1, "rsi50": rsi - 50, "macd_norm": macd / close,
            "boll_pos": (close - (ma20 - 2 * std20)) / (4 * std20),
            "vol_ratio": vol / vol.rolling(5).mean() - 1,
            "range_pos": (close - l60) / (h60 - l60),
        })
        parts.append(df)
    return pd.concat(parts, ignore_index=True)


def factor_screen(frames, train_fraction=0.6, horizon=10, min_abs_ic=0.03, test_start_date=None):
    panel = _factor_panel(frames, horizon)
    dates = sorted(panel["date"].unique())
    if len(dates) < 2:
        return {"ic_summary": [], "selected": [], "weights": {}, "signs": {},
                "training_cutoff_date": None, "training_signal_end_date": None,
                "training_target_end_date": None, "training_observation_count": 0,
                "error": "有效历史日期不足，无法生成动态因子模型。",
                "note": "有效历史日期不足，无法计算因子训练统计。"}
    cut_index = max(1, min(len(dates) - 1, int(len(dates) * train_fraction)))
    cutoff = _normalize_trade_date(test_start_date) if test_start_date else dates[cut_index]
    if cutoff is None:
        cutoff = dates[cut_index]
    # A training label is usable only when its full forward-return window ends strictly before the test start.
    train = panel[(panel["date"] < cutoff) & panel["target_date"].notna()
                  & (panel["target_date"] < cutoff)]
    training_signal_end = max(train["date"]) if not train.empty else None
    training_target_end = max(train["target_date"]) if not train.empty else None
    summary = []
    for k in FACTOR_NAMES:
        tmp = train[["ret", k]].dropna()
        if len(tmp) < 30:
            summary.append({"factor": k, "ic": None, "n": 0})
            continue
        ic = tmp[k].corr(tmp["ret"], method="spearman")
        summary.append({"factor": k, "ic": round(float(ic), 3), "n": int(len(tmp))})
    sel = [(x["factor"], x["ic"]) for x in summary if x["ic"] is not None and abs(x["ic"]) >= min_abs_ic]
    if not sel:
        ranked = sorted([x for x in summary if x["ic"] is not None], key=lambda x: abs(x["ic"]), reverse=True)[:4]
        sel = [(x["factor"], x["ic"]) for x in ranked]
    total = sum(abs(ic) for _, ic in sel)
    if not sel or total <= 1e-12:
        return {
            "ic_summary": summary,
            "selected": [],
            "weights": {},
            "signs": {},
            "training_cutoff_date": cutoff,
            "training_signal_end_date": training_signal_end,
            "training_target_end_date": training_target_end,
            "training_observation_count": int(len(train)),
            "error": "训练期没有可用的非零因子 IC，动态模型不生成。",
            "note": f"样本外起始日为 {cutoff}；训练期观察不足或因子 IC 均为0，未生成动态权重。",
        }
    weights = {k: round(abs(ic) / total, 4) for k, ic in sel}
    signs = {k: (1 if ic >= 0 else -1) for k, ic in sel}
    return {
        "ic_summary": summary,
        "selected": [{"factor": k, "ic": ic, "weight": weights[k], "sign": signs[k]} for k, ic in sel],
        "weights": weights,
        "signs": signs,
        "training_cutoff_date": cutoff,
        "training_signal_end_date": training_signal_end,
        "training_target_end_date": training_target_end,
        "training_observation_count": int(len(train)),
        "note": f"样本外起始日为 {cutoff}；只使用未来{horizon}日收益目标结束日期严格早于该日的训练行，计算池化秩相关(IC)。通过|IC|阈值的因子按|IC|加权；若没有因子达到0.03，则回退选择绝对IC最高的4项。",
    }


def _factor_values(sub):
    close, high, low, vol = sub["close"], sub["high"], sub["low"], sub["volume"]
    c = close.iloc[-1]
    if pd.isna(c):
        return None

    def last(x):
        return x.iloc[-1] if len(x) and pd.notna(x.iloc[-1]) else None

    rsi = _rsi14(close)
    macd = _macd_hist(close)
    ma20, std20 = close.rolling(20).mean(), close.rolling(20).std()
    h60, l60 = high.rolling(60).max(), low.rolling(60).min()
    h, l, m, s = last(h60), last(l60), last(ma20), last(std20)
    return {
        "mom_1": last(close.pct_change(1)),
        "mom_5": last(close.pct_change(5)),
        "mom_20": last(close.pct_change(20)),
        "mom_60": last(close.pct_change(60)),
        "trend": last(close / ma20 - 1),
        "rsi50": (last(rsi) - 50) if last(rsi) is not None else None,
        "macd_norm": (last(macd) / c) if last(macd) is not None else None,
        "boll_pos": ((c - (m - 2 * s)) / (4 * s)) if m is not None and s not in (None, 0) else None,
        "vol_ratio": (last(vol / vol.rolling(5).mean()) - 1) if last(vol.rolling(5).mean()) not in (None, 0) else None,
        "range_pos": ((c - l) / (h - l)) if h is not None and l is not None and h != l else None,
    }


def _rank_score(frames, d, weights, signs, factor_value_cache=None):
    if factor_value_cache is not None and d in factor_value_cache:
        vals = factor_value_cache[d]
    else:
        vals = {}
        for code, f in frames.items():
            sub = f[f["date"] <= d].tail(80)
            if len(sub) >= 25:
                fv = _factor_values(sub)
                if fv:
                    vals[code] = fv
        if factor_value_cache is not None:
            factor_value_cache[d] = vals
    scores = {c: 0.0 for c in vals}
    for k, w in weights.items():
        valid = [(c, vals[c].get(k)) for c in vals if vals[c].get(k) is not None and pd.notna(vals[c].get(k))]
        if len(valid) < 2:
            continue
        valid.sort(key=lambda x: x[1])
        rk = {c: i / (len(valid) - 1) for i, (c, _) in enumerate(valid)}
        for c in scores:
            scores[c] += w * signs[k] * (rk[c] if c in rk else 0.5)
    return scores


def _common_close_window(frames, start_fraction=0.0, end_fraction=1.0,
                         date_start=None, date_end_exclusive=None):
    """Use the same common-date sample for strategies and portfolio baselines."""
    if not frames:
        return pd.DataFrame()
    dates = sorted(set.intersection(*[set(f["date"]) for f in frames.values()]))
    if date_start is not None or date_end_exclusive is not None:
        dates = [d for d in dates if (date_start is None or d >= date_start)
                 and (date_end_exclusive is None or d < date_end_exclusive)]
    else:
        n0, n1 = int(len(dates) * start_fraction), int(len(dates) * end_fraction)
        dates = dates[n0:n1]
    if not dates:
        return pd.DataFrame()
    close = pd.DataFrame({c: f.set_index("date")["close"] for c, f in frames.items()})
    # Never turn a missing or nonpositive execution close into an invented fill.
    return close.reindex(dates).where(lambda values: values.gt(0)).dropna()


def _summarize(daily, curve, turnover_list, top_n, fee_rate, slippage_rate, drop_factor, hhi_list,
               dates=None, include_curve=False):
    ds = pd.Series(daily)
    n = len(ds)
    eq = curve[-1]
    ann = (eq ** (252 / n) - 1) * 100 if n and eq > 0 else 0.0
    vol = ds.std() * (252 ** 0.5) * 100 if n else 0.0
    sharpe = (ds.mean() * 252) / (ds.std() * (252 ** 0.5)) if n and ds.std() > 0 else 0.0
    s = pd.Series(curve)
    mdd = ((s.cummax() - s) / s.cummax()).max() * 100 if len(s) else 0.0
    at = sum(turnover_list) / (n / 252) if n else 0.0
    result = {
        "total_return_pct": round((eq - 1) * 100, 2),
        "annualized_return_pct": round(ann, 2),
        "sharpe": round(sharpe, 2),
        "max_drawdown_pct": round(mdd, 2),
        "annualized_volatility_pct": round(vol, 2),
        "turnover_per_year": round(at, 2),
        "weight_concentration_hhi": round(sum(hhi_list) / len(hhi_list), 3) if hhi_list else None,
        "holding_days": n,
        "start_date": str(dates[0]) if dates and n else None,
        "end_date": str(dates[min(n - 1, len(dates) - 1)]) if dates and n else None,
        "top_n": top_n,
        "fee_pct_per_side": round(fee_rate * 100, 4),
        "slippage_pct_per_side": round(slippage_rate * 100, 4),
        "sell_stamp_duty_pct_since_2023_08_28": STAMP_DUTY_RATE_AFTER * 100,
        "drop_factor": drop_factor,
    }
    if include_curve and dates and n:
        # Keep API responses compact while preserving the shape of the tested period.
        step = max(1, math.ceil(n / 120))
        result["equity_curve"] = [
            {"date": str(dates[i]), "value": round(float(curve[i + 1]) * 100, 4)}
            for i in range(0, min(n, len(dates)), step)
        ]
        if (n - 1) % step:
            result["equity_curve"].append(
                {"date": str(dates[n - 1]), "value": round(float(curve[-1]) * 100, 4)}
            )
    return result


def _backtest(frames, top_n, rebalance_days, fee_rate, slippage_rate, drop=None,
              start_fraction=0.0, end_fraction=1.0, include_curve=False,
              date_start=None, date_end_exclusive=None, min_dates=MIN_BACKTEST_DATES):
    close = _common_close_window(frames, start_fraction, end_fraction,
                                 date_start=date_start, date_end_exclusive=date_end_exclusive)
    if len(close) < min_dates:
        return None
    dates = close.index.tolist()
    holdings, daily, curve, hhi_list, turnover_list = {}, [], [1.0], [], []
    for i, d in enumerate(dates):
        holdings, ret = _marked_weights(holdings, close, i)
        turnover = 0.0
        buy_weight = sell_weight = 0.0
        # Signals use the prior close; the portfolio is changed at the current close.
        if i > 0 and (i - 1) % rebalance_days == 0 and i < len(dates) - 1:
            signal_date = dates[i - 1]
            scored = []
            for code, f in frames.items():
                sub = f[f["date"] <= signal_date].tail(60)
                if len(sub) >= 20:
                    ff = sa.calc_technical_indicators(sub.to_dict("records"))["factors"]
                    if drop:
                        w = {k: v for k, v in FIXED_WEIGHTS.items() if k != drop}
                        tot = sum(w.values()) or 1.0
                        s = sum(w[k] / tot * ff[k + "_score"] for k in w)
                    else:
                        s = sum(FIXED_WEIGHTS[k] * ff[k + "_score"] for k in FIXED_WEIGHTS)
                    scored.append((code, s))
            scored.sort(key=lambda x: x[1], reverse=True)
            top = scored[:top_n]
            ssum = sum(s for _, s in top)
            new_w = {c: s / ssum for c, s in top} if ssum else {}
            if new_w:
                buy_weight, sell_weight = _trade_sides(holdings, new_w)
                turnover = 0.5 * (buy_weight + sell_weight) if holdings else buy_weight
                holdings = new_w
                turnover_list.append(turnover)
                hhi_list.append(sum(w * w for w in holdings.values()))
        if i == len(dates) - 1 and holdings:
            # Include the cost of closing remaining positions at the end of the test.
            sell_weight += sum(holdings.values())
            turnover_list.append(sum(holdings.values()))
        net_equity = curve[-1] * (1 + ret) * (1 - _trade_cost_rate(
            buy_weight, sell_weight, d, fee_rate, slippage_rate))
        daily.append(net_equity / curve[-1] - 1)
        curve.append(net_equity)
    return _summarize(daily, curve, turnover_list, top_n, fee_rate, slippage_rate,
                      drop, hhi_list, dates=dates, include_curve=include_curve)


def _backtest_dynamic(frames, top_n, rebalance_days, fee_rate, slippage_rate, weights, signs,
                      start_fraction=0.0, end_fraction=1.0, include_curve=False,
                      date_start=None, date_end_exclusive=None, min_dates=MIN_BACKTEST_DATES,
                      factor_value_cache=None):
    close = _common_close_window(frames, start_fraction, end_fraction,
                                 date_start=date_start, date_end_exclusive=date_end_exclusive)
    if len(close) < min_dates:
        return None
    dates = close.index.tolist()
    holdings, daily, curve, hhi_list, turnover_list = {}, [], [1.0], [], []
    for i, d in enumerate(dates):
        holdings, ret = _marked_weights(holdings, close, i)
        turnover = 0.0
        buy_weight = sell_weight = 0.0
        # Signals use the prior close; the portfolio is changed at the current close.
        if i > 0 and (i - 1) % rebalance_days == 0 and i < len(dates) - 1:
            sc = _rank_score(frames, dates[i - 1], weights, signs, factor_value_cache)
            top = sorted(sc.items(), key=lambda x: x[1], reverse=True)[:top_n]
            positive = {c: max(0.0, float(s)) for c, s in top}
            ssum = sum(positive.values())
            if ssum > 0:
                new_w = {c: s / ssum for c, s in positive.items()}
            elif top:
                new_w = {c: 1.0 / len(top) for c, _ in top}
            else:
                new_w = {}
            if new_w:
                buy_weight, sell_weight = _trade_sides(holdings, new_w)
                turnover = 0.5 * (buy_weight + sell_weight) if holdings else buy_weight
                holdings = new_w
                turnover_list.append(turnover)
                hhi_list.append(sum(w * w for w in holdings.values()))
        if i == len(dates) - 1 and holdings:
            sell_weight += sum(holdings.values())
            turnover_list.append(sum(holdings.values()))
        net_equity = curve[-1] * (1 + ret) * (1 - _trade_cost_rate(
            buy_weight, sell_weight, d, fee_rate, slippage_rate))
        daily.append(net_equity / curve[-1] - 1)
        curve.append(net_equity)
    return _summarize(daily, curve, turnover_list, top_n, fee_rate, slippage_rate,
                      "dynamic", hhi_list, dates=dates, include_curve=include_curve)


def _backtest_equal_weight_hold(frames, fee_rate, slippage_rate, start_fraction=0.0,
                                end_fraction=1.0, include_curve=False,
                                date_start=None, date_end_exclusive=None):
    """Equal-weight buy-and-hold of the same pool, using the model's first close execution date."""
    close = _common_close_window(frames, start_fraction, end_fraction,
                                 date_start=date_start, date_end_exclusive=date_end_exclusive)
    if len(close) < 2 or not close.gt(0).all().all():
        return None
    dates = close.index.tolist()
    entry_close = close.iloc[1]
    relative_value = close.div(entry_close, axis=1).mean(axis=1)
    buy_cost = _trade_cost_rate(1.0, 0.0, dates[1], fee_rate, slippage_rate)
    sell_cost = _trade_cost_rate(0.0, 1.0, dates[-1], fee_rate, slippage_rate)
    curve, daily = [1.0], []
    for i in range(len(dates)):
        if i == 0:
            equity = 1.0
        else:
            equity = float(relative_value.iloc[i]) * (1 - buy_cost)
            if i == len(dates) - 1:
                equity *= 1 - sell_cost
        daily.append(equity / curve[-1] - 1)
        curve.append(equity)
    return _summarize(daily, curve, [1.0, 1.0], 0, fee_rate, slippage_rate,
                      "same_pool_equal_weight_hold", [1 / len(close.columns)],
                      dates=dates, include_curve=include_curve)


def _fetch_market_benchmark(dates):
    """Fetch CSI All Share index closes and require exact coverage of the test dates."""
    if len(dates) < 2:
        return None, "样本外基准日期不足", None, None
    source = "AKShare stock_zh_index_daily_em（东方财富）"
    try:
        date_keys = [pd.Timestamp(date).strftime("%Y-%m-%d") for date in dates]
        frame = sa.ak.stock_zh_index_daily_em(
            symbol=MARKET_BENCHMARK["symbol"],
            start_date=pd.Timestamp(date_keys[0]).strftime("%Y%m%d"),
            end_date=pd.Timestamp(date_keys[-1]).strftime("%Y%m%d"),
        )
        if frame is None or frame.empty or not {"date", "close"}.issubset(frame.columns):
            raise ValueError("接口没有返回所需的日期和收盘点位")
        frame = frame[["date", "close"]].copy()
        frame["date"] = frame["date"].map(_normalize_trade_date)
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame = frame.dropna(subset=["date", "close"])
        frame = frame.loc[frame["close"] > 0].drop_duplicates("date", keep="last")
        close = frame.set_index("date")["close"]
        missing = [date for date in date_keys if date not in close.index]
        if missing:
            retrieved_at = datetime.now().astimezone().isoformat(timespec="seconds")
            return None, f"中证全指缺少 {len(missing)} 个样本外交易日的收盘点位，未用前值填充", source, retrieved_at
        retrieved_at = datetime.now().astimezone().isoformat(timespec="seconds")
        return close.reindex(date_keys).astype(float), None, source, retrieved_at
    except Exception as exc:
        retrieved_at = datetime.now().astimezone().isoformat(timespec="seconds")
        return None, f"中证全指基准获取失败：{type(exc).__name__}: {str(exc)[:140]}", source, retrieved_at


def _market_index_summary(dates, close, include_curve=False):
    if len(dates) < 2 or close is None:
        return None
    close = close.reindex(dates)
    if close.isna().any() or (close <= 0).any():
        return None
    entry_close = float(close.iloc[1])
    curve, daily = [1.0], []
    for i in range(len(dates)):
        equity = 1.0 if i < 2 else float(close.iloc[i]) / entry_close
        daily.append(equity / curve[-1] - 1)
        curve.append(equity)
    result = _summarize(daily, curve, [], 0, 0.0, 0.0, None, [],
                        dates=dates, include_curve=include_curve)
    result.update({"return_basis": "指数点位收益；未扣除分红、基金费用、交易成本或跟踪误差",
                   "cost_applied": False})
    return result


def _backtest_market_index(frames, start_fraction=0.0, end_fraction=1.0, include_curve=False):
    close_window = _common_close_window(frames, start_fraction, end_fraction)
    if len(close_window) < MIN_BACKTEST_DATES:
        return None, f"样本外共同交易日不足{MIN_BACKTEST_DATES}天", None, None, None
    dates = close_window.index.tolist()
    close, error, source, retrieved_at = _fetch_market_benchmark(dates)
    if error:
        return None, error, source, retrieved_at, None
    result = _market_index_summary(dates, close, include_curve=include_curve)
    if result is None:
        return None, "中证全指基准价格序列无法与测试日期对齐", source, retrieved_at, None
    result["source_retrieved_at"] = retrieved_at
    return result, None, source, retrieved_at, close


def _rolling_fold_rollup(folds, key):
    results = [fold[key] for fold in folds if isinstance(fold.get(key), dict)]
    returns = [item["total_return_pct"] for item in results if item.get("total_return_pct") is not None]
    drawdowns = [item["max_drawdown_pct"] for item in results if item.get("max_drawdown_pct") is not None]
    return {
        "available_fold_count": len(results),
        "positive_fold_count": sum(value > 0 for value in returns),
        "median_total_return_pct": round(float(pd.Series(returns).median()), 2) if returns else None,
        "median_max_drawdown_pct": round(float(pd.Series(drawdowns).median()), 2) if drawdowns else None,
    }


def _rolling_oos_analysis(frames, top_n, rebalance_days, fee_rate, slippage_rate,
                          market_close=None, market_error=None):
    """Four predeclared, non-overlapping folds in the original 40% holdout."""
    close = _common_close_window(frames)
    dates = close.index.tolist()
    first_dates = {code: frame["date"].min() for code, frame in frames.items() if not frame.empty}
    last_dates = {code: frame["date"].max() for code, frame in frames.items() if not frame.empty}
    latest_start = max(first_dates.values()) if first_dates else None
    earliest_end = min(last_dates.values()) if last_dates else None
    start_limited_codes = sorted(code for code, date in first_dates.items() if date == latest_start)
    end_limited_codes = sorted(code for code, date in last_dates.items() if date == earliest_end)
    split = int(len(dates) * 0.6)
    test_count = len(dates) - split
    bounds = [split + (test_count * i // ROLLING_FOLD_COUNT)
              for i in range(ROLLING_FOLD_COUNT + 1)]
    folds = []
    for fold_no in range(ROLLING_FOLD_COUNT):
        start_idx, end_idx = bounds[fold_no], bounds[fold_no + 1]
        row = {"fold": fold_no + 1, "test_start_date": None, "test_end_date": None,
               "training_cutoff_date": None, "training_signal_end_date": None,
               "training_target_end_date": None, "training_observation_count": 0,
               "fixed": None, "dynamic": None, "same_pool_equal_weight": None,
               "market_index": None, "status": "unavailable", "error": None}
        if start_idx >= end_idx:
            row["error"] = "样本外共同交易日不足，无法划出非空测试窗口"
            folds.append(row)
            continue
        window_dates = dates[start_idx:end_idx]
        date_start = window_dates[0]
        date_end_exclusive = dates[end_idx] if end_idx < len(dates) else None
        row.update({"test_start_date": date_start, "test_end_date": window_dates[-1],
                    "training_cutoff_date": date_start,
                    "test_session_count": len(window_dates)})
        if len(window_dates) < MIN_BACKTEST_DATES:
            row["error"] = (f"窗口只有{len(window_dates)}个共同交易日；"
                            f"当前回测最低要求{MIN_BACKTEST_DATES}天，未计算该段指标")
            folds.append(row)
            continue

        screen = factor_screen(frames, test_start_date=date_start)
        row.update({
            "training_signal_end_date": screen.get("training_signal_end_date"),
            "training_target_end_date": screen.get("training_target_end_date"),
            "training_observation_count": screen.get("training_observation_count", 0),
            "dynamic_error": screen.get("error"),
        })
        row["fixed"] = _backtest(
            frames, top_n, rebalance_days, fee_rate, slippage_rate,
            date_start=date_start,
            date_end_exclusive=date_end_exclusive,
        )
        if screen.get("weights"):
            row["dynamic"] = _backtest_dynamic(
                frames, top_n, rebalance_days, fee_rate, slippage_rate,
                screen["weights"], screen["signs"],
                date_start=date_start, date_end_exclusive=date_end_exclusive,
            )
        row["same_pool_equal_weight"] = _backtest_equal_weight_hold(
            frames, fee_rate, slippage_rate,
            date_start=date_start, date_end_exclusive=date_end_exclusive,
        )
        if market_error:
            row["market_index_error"] = market_error
        elif market_close is not None:
            row["market_index"] = _market_index_summary(
                window_dates, market_close,
            )
        else:
            row["market_index_error"] = "未取得中证全指基准数据"
        row["status"] = "available" if row["fixed"] is not None else "unavailable"
        if row["fixed"] is None:
            row["error"] = "该段固定模型回测未生成"
        folds.append(row)

    summary = {
        "fixed": _rolling_fold_rollup(folds, "fixed"),
        "dynamic": _rolling_fold_rollup(folds, "dynamic"),
        "same_pool_equal_weight": _rolling_fold_rollup(folds, "same_pool_equal_weight"),
        "market_index": _rolling_fold_rollup(folds, "market_index"),
    }
    for benchmark_key in ("same_pool_equal_weight", "market_index"):
        comparable = [(fold.get("dynamic"), fold.get(benchmark_key)) for fold in folds]
        comparable = [(model, benchmark) for model, benchmark in comparable
                      if isinstance(model, dict) and isinstance(benchmark, dict)
                      and model.get("total_return_pct") is not None
                      and benchmark.get("total_return_pct") is not None]
        summary[f"dynamic_beats_{benchmark_key}_count"] = sum(
            model["total_return_pct"] > benchmark["total_return_pct"]
            for model, benchmark in comparable
        )
        summary[f"dynamic_comparable_{benchmark_key}_fold_count"] = len(comparable)
    return {
        "fold_count_requested": ROLLING_FOLD_COUNT,
        "initial_training_fraction": 0.6,
        "test_fraction": 0.4,
        "common_trading_day_count": len(dates),
        "common_start_date": dates[0] if dates else None,
        "common_end_date": dates[-1] if dates else None,
        "minimum_common_dates_for_all_folds": MIN_COMMON_DATES_FOR_ROLLING,
        "pool_latest_first_quote_date": latest_start,
        "pool_earliest_last_quote_date": earliest_end,
        "start_limited_codes": start_limited_codes,
        "end_limited_codes": end_limited_codes,
        "available_fold_count": sum(fold["status"] == "available" for fold in folds),
        "summary": summary,
        "folds": folds,
        "note": ("先按共同交易日期前60%作为初始训练区，再将后40%等分为4个连续、互不重叠窗口；"
                 "每段开始时用此前所有可用数据扩展训练，并要求10日收益标签在该段开始日前结束。"
                 "每一段单独从现金开始并在段末平仓，收益不能拼成一条连续可交易净值；"
                 f"每段至少需要{MIN_BACKTEST_DATES}个共同交易日。四段结果只是描述性小样本，不能证明模型稳定或盈利。"),
    }


def portfolio_analysis(pool=None, top_n=5, rebalance_days=20, fee_pct=0.1, slippage_pct=0.05, progress=None):
    """组合历史检验；输入费率不含另计的卖出印花税。"""
    fee_rate = float(fee_pct) / 100
    slippage_rate = float(slippage_pct) / 100
    if (not math.isfinite(fee_rate) or not math.isfinite(slippage_rate)
            or not 0 <= fee_rate <= 0.01 or not 0 <= slippage_rate <= 0.01):
        raise ValueError("单边券商及其他费用假设、滑点都需在0至1%之间")
    pool = pool or sa.DEFAULT_POOL
    if progress:
        progress("fetch", 0, len(pool), "并行获取股票历史行情")
    frames, lineage, completed_before = _fetch(pool, progress=progress)
    if len(frames) < 3:
        return {"error": f"样本不足，无法组合：请求{len(pool)}只股票，成功取到{len(frames)}只行情，至少需要3只。"}
    if progress:
        progress("backtest", 0, 5, f"已取到 {len(frames)} 只股票，开始计算策略历史表现")
    full = _backtest(frames, top_n, rebalance_days, fee_rate, slippage_rate)
    if full is None:
        common = _common_close_window(frames)
        first_dates = {code: frame["date"].min() for code, frame in frames.items() if not frame.empty}
        last_dates = {code: frame["date"].max() for code, frame in frames.items() if not frame.empty}
        latest_start = max(first_dates.values()) if first_dates else None
        earliest_end = min(last_dates.values()) if last_dates else None
        start_limited = sorted(code for code, date in first_dates.items() if date == latest_start)
        end_limited = sorted(code for code, date in last_dates.items() if date == earliest_end)
        common_range = (f"{common.index[0]} 至 {common.index[-1]}" if not common.empty else "无")
        limiter_text = (f"最晚首条行情为{latest_start}（{', '.join(start_limited)}）；"
                        f"最早末条行情为{earliest_end}（{', '.join(end_limited)}）" if start_limited else "没有可用的共同日期")
        return {"error": (f"历史数据不足：成功取到{len(frames)}/{len(pool)}只；全池共同交易日{len(common)}天，"
                           f"回测至少需要{MIN_BACKTEST_DATES}天；共同日期范围{common_range}。{limiter_text}可能压缩了历史。")}
    margin = []
    for drop in ("trend", "momentum", "volatility"):
        r = _backtest(frames, top_n, rebalance_days, fee_rate, slippage_rate, drop=drop)
        if r is not None:
            margin.append({
                "factor": drop,
                "return_without_pct": r["total_return_pct"],
                "marginal_contribution_pct": round(full["total_return_pct"] - r["total_return_pct"], 2),
            })
        if progress:
            progress("backtest", len(margin) + 1, 5, "比较不同技术因子对策略结果的影响")
    if progress:
        progress("validation", 4, 5, "正在做按时间先后划分的样本外验证")
    oos_window = _common_close_window(frames, 0.6, 1.0)
    test_start_date = str(oos_window.index[0]) if len(oos_window) else None
    screen = factor_screen(frames, test_start_date=test_start_date)
    old_train = _backtest(frames, top_n, rebalance_days, fee_rate, slippage_rate, start_fraction=0.0, end_fraction=0.6)
    old_test = _backtest(frames, top_n, rebalance_days, fee_rate, slippage_rate,
                         start_fraction=0.6, end_fraction=1.0, include_curve=True)
    oos_factor_value_cache = {}
    if screen["weights"]:
        dyn_train = _backtest_dynamic(frames, top_n, rebalance_days, fee_rate, slippage_rate,
                                      screen["weights"], screen["signs"], 0.0, 0.6)
        dyn_test = _backtest_dynamic(frames, top_n, rebalance_days, fee_rate, slippage_rate,
                                     screen["weights"], screen["signs"], 0.6, 1.0,
                                     include_curve=True, factor_value_cache=oos_factor_value_cache)
    else:
        dyn_train = dyn_test = None
    dynamic_ablation = []
    if dyn_test is not None:
        for factor in screen["weights"]:
            remaining = {k: weight for k, weight in screen["weights"].items() if k != factor}
            total_weight = sum(remaining.values())
            if total_weight <= 0:
                dynamic_ablation.append({"factor": factor, "status": "unavailable",
                                         "reason": "仅选中一个因子，无法留一"})
                continue
            normalized = {k: weight / total_weight for k, weight in remaining.items()}
            reduced = _backtest_dynamic(
                frames, top_n, rebalance_days, fee_rate, slippage_rate,
                normalized, {k: screen["signs"][k] for k in remaining}, 0.6, 1.0,
                factor_value_cache=oos_factor_value_cache,
            )
            dynamic_ablation.append({
                "factor": factor,
                "status": "available" if reduced is not None else "unavailable",
                "full_oos_return_pct": dyn_test["total_return_pct"],
                "without_oos_return_pct": reduced["total_return_pct"] if reduced else None,
                "difference_pct_points": round(dyn_test["total_return_pct"] - reduced["total_return_pct"], 2)
                if reduced else None,
                "reason": None if reduced else "样本外日期不足",
            })
    same_pool_test = _backtest_equal_weight_hold(
        frames, fee_rate, slippage_rate, start_fraction=0.6, end_fraction=1.0,
        include_curve=True,
    )
    if progress:
        progress("benchmark", 4, 5, "正在获取中证全指基准并核对样本外日期")
    market_test, market_error, market_source, market_retrieved_at, market_close = _backtest_market_index(
        frames, start_fraction=0.6, end_fraction=1.0, include_curve=True,
    )
    if progress:
        progress("rolling-validation", 4, 5, "正在计算四段预设滚动样本外窗口")
    rolling_oos = _rolling_oos_analysis(
        frames, top_n, rebalance_days, fee_rate, slippage_rate,
        market_close=market_close, market_error=market_error,
    )
    result = {
        "data_coverage": {
            "requested_pool_count": len(pool),
            "used_pool_count": len(frames),
            "missing_codes": sorted(set(pool) - set(frames)),
            "completed_session_cutoff_exclusive": completed_before,
            "per_code": lineage,
            "zero_volume_rows": sum(int(frame["volume"].eq(0).sum()) for frame in frames.values()),
            "zero_volume_codes": sorted(code for code, frame in frames.items()
                                        if frame["volume"].eq(0).any()),
        },
        "cost_assumptions": {
            "broker_and_other_fee_pct_per_side": round(fee_rate * 100, 4),
            "slippage_pct_per_side": round(slippage_rate * 100, 4),
            "sell_stamp_duty_pct_since_2023_08_28": STAMP_DUTY_RATE_AFTER * 100,
            "sell_stamp_duty_pct_before_2023_08_28": STAMP_DUTY_RATE_BEFORE * 100,
            "stamp_duty_change_date": STAMP_DUTY_CHANGE_DATE,
            "stamp_duty_source": "财政部 税务总局公告2023年第39号；上海证券交易所股票投资收费说明",
            "fixed_minimum_commission_modeled": False,
        },
        "full": full,
        "factor_marginal_contribution": margin,
        "dynamic_factor_ablation": {
            "method": "按训练期所选权重依次移除一个因子，将余下权重归一，在同一后40%留出日期和同一成本口径回测；差值不是因果贡献。",
            "rows": dynamic_ablation,
            "error": screen.get("error") if dyn_test is None else None,
        },
        "factor_screen": screen,
        "comparison": {
            "old": {"in_sample": old_train, "out_of_sample": old_test},
            "dynamic": {"in_sample": dyn_train, "out_of_sample": dyn_test,
                        "error": screen.get("error")},
        },
        "rolling_oos": rolling_oos,
        "benchmarks": {
            "same_pool_equal_weight": {
                "name": "同池等权买入持有",
                "out_of_sample": same_pool_test,
                "cost_applied": True,
                "note": "同一持仓池与样本外日期；在策略首次假定成交的收盘时点等权买入，样本结束时卖出，按相同双边费用、滑点与卖出印花税计算。",
            },
            "market_index": {
                "name": MARKET_BENCHMARK["name"],
                "code": MARKET_BENCHMARK["code"],
                "source": market_source,
                "source_retrieved_at": market_retrieved_at,
                "out_of_sample": market_test,
                "error": market_error,
                "cost_applied": False,
                "note": "指数点位收益参照；不代表可直接交易产品收益，也未扣除分红、基金费用、交易成本或跟踪误差。",
            },
        },
        "note": (f"old=固定权重(趋势/动量/波动)模型；dynamic=训练期IC筛选+动态加权模型；"
                 f"均按top_n只分数加权，券商及其他双边费用假设单边{fee_rate * 100:g}%、滑点单边{slippage_rate * 100:g}%，"
                 "另按交易日对卖出额计证券交易印花税（2023-08-28起0.05%，此前0.1%），并计入测试结束时平仓成本；"
                 "调仓前按当日收盘价更新持仓权重；信号取前一交易日收盘数据、于下一交易日收盘假设成交；样本外验证按前60%训练后40%测试。"
                 "新增滚动验证把60/40样本外区间分为4个互不重叠窗口，每段独立从现金起算并计入开仓/平仓成本，不拼接为连续实盘净值。"
                 "组合行情取数范围为近1095个自然日，只采用上海日期中今天以前的日线；共同交易日期交集仍可能被上市较晚或缺失较多的证券缩短。"
                 "同池等权基准使用相同样本外日期和成本；中证全指为指数点位参照，不计交易成本与分红。"
                 "未逐笔模拟最低佣金、整手交易、停牌、涨跌停排队和盘口流动性；零成交量行仅提示，不能证明假定交易可成交。"),
    }
    if progress:
        progress("done", 5, 5, "策略历史检验完成")
    return result
