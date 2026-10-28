# -*- coding: utf-8 -*-
"""持仓池：本地JSON存储 + 实时盈亏/10日均线计算。"""
import json
import math
import os
import threading
import time
import concurrent.futures
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import public_quote_http
import pandas as pd
import stock_agent as sa
import private_storage
from functools import wraps

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HOLDINGS_FILE = os.path.join(BASE_DIR, "holdings.json")
_CACHE_LOCK = threading.Lock()
_REALTIME_CACHE = {}
_MA10_CACHE = {}
_QUOTES_CACHE = {"at": 0.0, "rows": None}
_WRITE_LOCK = threading.RLock()


def _serialized(function):
    @wraps(function)
    def guarded(*args, **kwargs):
        with _WRITE_LOCK:
            return function(*args, **kwargs)
    return guarded


def load():
    return private_storage.read(HOLDINGS_FILE, [])


def save(items):
    private_storage.write(HOLDINGS_FILE, items)
    with _CACHE_LOCK:
        _QUOTES_CACHE.update({"at": 0.0, "rows": None})


@_serialized
def add(code, cost_price, shares, acquired_on=None):
    items = load()
    nid = max([x.get("id", 0) for x in items], default=0) + 1
    item = {"id": nid, "code": code, "cost_price": float(cost_price), "shares": float(shares),
            "acquired_on": acquired_on or None}
    items.append(item)
    save(items)
    return item


@_serialized
def update(hid, cost_price=None, shares=None, acquired_on=None):
    items = load()
    for x in items:
        if x.get("id") == hid:
            if cost_price is not None:
                x["cost_price"] = float(cost_price)
            if shares is not None:
                x["shares"] = float(shares)
            if acquired_on is not None:
                x["acquired_on"] = acquired_on or None
            save(items)
            return x
    return None


@_serialized
def delete(hid):
    items = load()
    kept = [x for x in items if x.get("id") != hid]
    save(kept)
    return len(kept) != len(items)


def _realtime(code):
    """腾讯报价；保留供应商时间，不能把盘后旧报价称为当下价格。"""
    now = time.monotonic()
    with _CACHE_LOCK:
        hit = _REALTIME_CACHE.get(code)
        if hit and now - hit[0] < 8:
            return hit[1]
    try:
        r = public_quote_http.get(f"https://qt.gtimg.cn/q={sa._prefix(code)}{code}", timeout=5)
        r.encoding = "gbk"
        parts = r.text.split("~")
        if len(parts) > 30 and parts[2] == str(code):
            price = float(parts[3])
            prev = float(parts[4]) if len(parts) > 4 else None
            if math.isfinite(price) and price > 0:
                try:
                    source_time = datetime.strptime(parts[30], "%Y%m%d%H%M%S").replace(
                        tzinfo=ZoneInfo("Asia/Shanghai"))
                    if source_time > datetime.now(ZoneInfo("Asia/Shanghai")):
                        source_time = None
                except (TypeError, ValueError):
                    source_time = None
                result = (price, (parts[1] if len(parts) > 1 else None), prev,
                          source_time.isoformat(timespec="seconds") if source_time else None,
                          datetime.now().astimezone().isoformat(timespec="seconds"))
                with _CACHE_LOCK:
                    _REALTIME_CACHE[code] = (now, result)
                return result
    except Exception:
        pass
    result = (None, None, None, None, None)
    with _CACHE_LOCK:
        _REALTIME_CACHE[code] = (now, result)
    return result


def public_quote(code, at=None):
    """One public quote with explicit time semantics; never reads holdings."""
    import market_clock
    code=str(code).strip()
    if not code.isdigit() or len(code)!=6:raise ValueError('请输入6位股票代码')
    price,name,previous,source_time,retrieved=_realtime(code)
    evidence=market_clock.quote_evidence(source_time,retrieved,at)
    valid=price is not None and math.isfinite(float(price)) and price>0
    if not valid:
        evidence['usable_for_valuation']=False
        evidence['warnings'].append('供应商价格暂不可用')
    previous=previous if previous is not None and math.isfinite(previous) and previous>0 else None
    return {'code':code,'name':name or code,'price':price if valid else None,'previous_close':previous,
        'source':'tencent','source_time':source_time,'retrieved_at':retrieved,'price_evidence':evidence,
        'market_time':evidence['market_time']}


def _ma10(code):
    now = time.monotonic()
    with _CACHE_LOCK:
        hit = _MA10_CACHE.get(code)
        if hit and now - hit[0] < 90:
            return hit[1]
    end = pd.Timestamp.today().strftime("%Y%m%d")
    start = (pd.Timestamp.today() - pd.Timedelta(days=60)).strftime("%Y%m%d")
    df = sa._kline_fast_for_user(code, start, end)
    if df is None or df.empty:
        result = (None, None, None, None, None)
    else:
        close = pd.to_numeric(df["close"], errors="coerce")
        valid = close.dropna()
        last_index = valid.index[-1] if len(valid) else None
        ma = close.rolling(10).mean().loc[last_index] if last_index is not None else None
        last_date = pd.to_datetime(df.loc[last_index, "date"], errors="coerce") if last_index is not None else None
        result = (float(valid.iloc[-1]) if len(valid) else None,
                  float(ma) if ma is not None and pd.notna(ma) else None,
                  last_date.strftime("%Y-%m-%d") if last_date is not None and pd.notna(last_date) else None,
                  df.attrs.get("source"), df.attrs.get("source_retrieved_at"))
    with _CACHE_LOCK:
        _MA10_CACHE[code] = (now, result)
    return result


def _quote_one(h, names):
    code = h["code"]
    try:
        rt, rt_name, _prev, rt_source_time, rt_retrieved_at = _realtime(code)
    except Exception:
        rt, rt_name, rt_source_time, rt_retrieved_at = None, None, None, None
    try:
        last_close, ma10, daily_date, daily_source, daily_retrieved_at = _ma10(code)
    except Exception:
        last_close, ma10, daily_date, daily_source, daily_retrieved_at = None, None, None, None, None
    rt_date = rt_source_time[:10] if rt_source_time else None
    use_rt = rt is not None and (daily_date is None or (rt_date is not None and rt_date >= daily_date))
    price = rt if use_rt else last_close
    price_date = rt_date if use_rt else daily_date
    retrieved_at = rt_retrieved_at if use_rt else daily_retrieved_at
    source = "realtime" if use_rt else "daily_close" if last_close is not None else None
    today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    def age_days(value):
        try:
            return (today - datetime.fromisoformat(value).date()).days if value else None
        except ValueError:
            return None
    price_age = age_days(price_date)
    ma_age = age_days(daily_date)
    quote_warning = ("报价日期未知" if price is not None and price_age is None else
                     "报价日期异常" if price_age is not None and price_age < 0 else
                     "报价已超过7个自然日" if price_age is not None and price_age > 7 else
                     "10日均线暂不可用" if ma10 is None else
                     "均线日期未知或过期" if ma10 is not None and (ma_age is None or ma_age < 0 or ma_age > 7) else None)
    cost, shares = float(h.get("cost_price", 0)), float(h.get("shares", 0))
    name = rt_name or names.get(code) or code
    if price is None:
        return {**h, "name": name, "price": None, "price_source": None,
                "price_trade_date": None, "price_source_time": None,
                "price_retrieved_at": None, "daily_source": daily_source,
                "price_age_calendar_days": None, "ma10_trade_date": daily_date,
                "quote_warning": "行情暂不可用", "ma10": ma10,
                "market_value": None, "pnl": None, "pnl_pct": None, "below_ma10": False,
                "quote_error": "行情暂不可用"}
    pnl = (price - cost) * shares
    return {**h, "name": name, "price": round(price, 2),
            "price_source": source, "price_trade_date": price_date,
            "price_source_time": rt_source_time if use_rt else None,
            "price_retrieved_at": retrieved_at, "daily_source": daily_source,
            "price_age_calendar_days": price_age, "ma10_trade_date": daily_date,
            "quote_warning": quote_warning,
            "ma10": round(ma10, 2) if ma10 is not None else None,
            "market_value": round(price * shares, 2), "pnl": round(pnl, 2),
            "pnl_pct": round((price / cost - 1) * 100, 2) if cost else 0.0,
            "below_ma10": bool(ma10 is not None and quote_warning is None and price < ma10),
            "quote_error": None}


def _with_price_time(row):
    import market_clock
    evidence=(market_clock.quote_evidence(row.get('price_source_time'),row.get('price_retrieved_at'))
              if row.get('price_source')=='realtime' else
              market_clock.daily_evidence(row.get('price_trade_date'),row.get('price_retrieved_at')))
    return {**row,'price_evidence':evidence,'market_time':market_clock.snapshot()}


def quotes(with_cache_state=False):
    now = time.monotonic()
    with _CACHE_LOCK:
        if _QUOTES_CACHE["rows"] is not None and now - _QUOTES_CACHE["at"] < 12:
            rows = [_with_price_time(row) for row in _QUOTES_CACHE["rows"]]
            return (rows, "memory") if with_cache_state else rows
    items = load()
    if not items:
        return ([], "unknown") if with_cache_state else []
    names, _, _ = sa._name_map_local()
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(items))) as pool:
        out = list(pool.map(lambda h: _quote_one(h, names), items))
    with _CACHE_LOCK:
        _QUOTES_CACHE.update({"at": now, "rows": [dict(row) for row in out]})
    out = [_with_price_time(row) for row in out]
    return (out, "provider") if with_cache_state else out
