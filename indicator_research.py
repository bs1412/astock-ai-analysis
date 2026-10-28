"""Research-only, past-bar indicators. No trading signal or ranking integration."""

import math

import pandas as pd


def atr_normalized_ma_slope(rows):
    """Return five-session MA20 slope in units of current ATR14 per session.

    Caller must supply consecutive, completed daily bars in a single adjustment
    convention. Values are descriptive and have no calibrated trading threshold.
    """
    frame = pd.DataFrame(rows)
    required = {"date", "high", "low", "close"}
    if not required.issubset(frame.columns) or len(frame) < 25:
        return {"status": "insufficient", "reason": "需要至少25根完整日线及日期、高、低、收盘"}
    dates = pd.to_datetime(frame["date"], errors="coerce")
    prices = frame[["high", "low", "close"]].apply(pd.to_numeric, errors="coerce")
    if (dates.isna().any() or not dates.is_monotonic_increasing or dates.duplicated().any()
            or prices.isna().any().any()
            or not prices.map(math.isfinite).all().all()
            or (prices <= 0).any().any()
            or (prices["high"] < prices[["low", "close"]].max(axis=1)).any()
            or (prices["low"] > prices["close"]).any()):
        return {"status": "invalid", "reason": "日期或价格序列无效"}
    close, high, low = prices["close"], prices["high"], prices["low"]
    prior_close = close.shift(1)
    true_range = pd.concat((high - low, (high - prior_close).abs(),
                            (low - prior_close).abs()), axis=1).max(axis=1)
    ma20 = close.rolling(20, min_periods=20).mean()
    atr14 = true_range.rolling(14, min_periods=14).mean()
    current_atr = float(atr14.iloc[-1])
    if current_atr <= 0:
        return {"status": "undefined", "reason": "ATR14 为零，无法归一化"}
    return {"status": "ok", "date": dates.iloc[-1].date().isoformat(),
            "ma20": float(ma20.iloc[-1]), "ma20_5_sessions_ago": float(ma20.iloc[-6]),
            "atr14": current_atr,
            "value": float((ma20.iloc[-1] - ma20.iloc[-6]) / (5 * current_atr)),
            "unit": "ATR14_per_session", "use": "research_only"}
