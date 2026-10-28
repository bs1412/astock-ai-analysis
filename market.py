# -*- coding: utf-8 -*-
"""大盘环境：三大指数实时点位 + 基于最近收盘数据的描述性趋势状态。"""
import requests
import public_quote_http
import pandas as pd
import akshare as ak
import threading
import time
from datetime import datetime
import market_clock

_LOCK = threading.Lock()
_INDEX_CACHE = {"at": 0.0, "rows": None}
_REGIME_CACHE = {"at": 0.0, "data": None, "running": False}


def _indexes():
    now = time.monotonic()
    with _LOCK:
        if _INDEX_CACHE["rows"] is not None and now - _INDEX_CACHE["at"] < 15:
            return list(_INDEX_CACHE["rows"])
    r = public_quote_http.get("https://qt.gtimg.cn/q=sh000001,sz399001,sz399006", timeout=8)
    r.encoding = "gbk"
    out = []
    for line in r.text.strip().split(";"):
        if "=" not in line:
            continue
        p = line.split("=", 1)[1].strip().strip('"').split("~")
        if len(p) < 5:
            continue
        price, prev = float(p[3]), float(p[4])
        try:source_time=datetime.strptime(p[30],'%Y%m%d%H%M%S').replace(tzinfo=market_clock.SHANGHAI).isoformat()
        except (ValueError,IndexError):source_time=None
        out.append({
            "code": p[2], "name": p[1], "price": round(price, 2),
            "change_pct": round((price / prev - 1) * 100, 2) if prev else 0.0,
            "source_time":source_time,"retrieved_at":market_clock.now().isoformat(timespec='seconds'),
        })
    with _LOCK:
        _INDEX_CACHE.update({"at": now, "rows": list(out)})
    return out


def _compute_regime():
    try:
        d = ak.stock_zh_index_daily(symbol="sh000001").tail(80)
        d=d.copy();d['date']=pd.to_datetime(d['date'],errors='coerce')
        d=d.loc[d['date'].notna() & d['date'].dt.date.lt(market_clock.now().date())]
        d['close']=pd.to_numeric(d['close'],errors='coerce')
        d=d.loc[d['close'].notna() & d['close'].gt(0)].sort_values('date')
        close = pd.to_numeric(d["close"], errors="coerce")
        clean = close.dropna()
        if len(clean) < 21:
            return {"label": "数据暂缺", "ic": None, "note": "指数历史数据不足21个交易日"}
        latest = float(clean.iloc[-1])
        ma20 = float(clean.tail(20).mean())
        mom20 = (latest / float(clean.iloc[-21]) - 1) * 100
        distance = (latest / ma20 - 1) * 100 if ma20 else 0.0
        if latest >= ma20 and mom20 > 0:
            label = "指数在20日均线上方，近20日上涨"
        elif latest < ma20 and mom20 < 0:
            label = "指数在20日均线下方，近20日走弱"
        else:
            label = "指数短期方向信号不一致"
        dates = d["date"] if "date" in d.columns else None
        as_of = str(dates.iloc[-1])[:10] if dates is not None and len(dates) else None
        return {
            "label": label, "ic": None,
            "mom20_pct": round(mom20, 2),
            "distance_ma20_pct": round(distance, 2),
            "as_of": as_of,
            "price_evidence":market_clock.daily_evidence(as_of),
            "note": "按所列日期已结束日线收盘参考、20日均线和近20日涨幅描述；不把当日盘中价作为已收盘依据，不代表未来走势或交易信号",
        }
    except Exception:
        return {"label": "数据暂缺", "ic": None, "note": ""}


def _refresh_regime():
    try:
        data = _compute_regime()
    except Exception:
        data = {"label": "数据暂缺", "ic": None, "note": "市场历史数据暂不可用"}
    with _LOCK:
        _REGIME_CACHE.update({"at": time.monotonic(), "data": data, "running": False})


def _regime():
    now = time.monotonic()
    with _LOCK:
        data = _REGIME_CACHE["data"]
        fresh = data is not None and now - _REGIME_CACHE["at"] < (1800 if data.get("mom20_pct") is not None else 180)
        if fresh or _REGIME_CACHE["running"]:
            return dict(data or {"label": "市场特征计算中", "ic": None,
                                 "note": "正在后台计算历史市场状态，请稍后刷新"})
        _REGIME_CACHE["running"] = True
    try:
        threading.Thread(target=_refresh_regime, daemon=True).start()
    except Exception:
        with _LOCK:
            _REGIME_CACHE["running"] = False
        return data or {"label": "数据暂缺", "ic": None, "note": "无法启动市场特征计算"}
    return data or {"label": "市场特征计算中", "ic": None,
                    "note": "正在后台计算历史市场状态，请稍后刷新"}


def market():
    index_error = None
    try:
        indexes = _indexes()
    except Exception:
        indexes = []
        index_error = "指数行情暂不可用，请稍后刷新"
    indexes=[{**row,'price_evidence':market_clock.quote_evidence(row.get('source_time'),row.get('retrieved_at'))} for row in indexes]
    return {"indexes": indexes, "regime": _regime(), "index_error": index_error,'market_time':market_clock.snapshot()}
