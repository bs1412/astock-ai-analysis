# -*- coding: utf-8 -*-
"""A股严格分析Agent：多维数据 -> 量化评分+回测 -> DeepSeek两阶段工具调用 -> 风控复核 -> 终稿。仅供投资参考，不构成建议。"""
import json
import os
import sys
import time
from datetime import datetime

import requests
import pandas as pd
import akshare as ak
import config
import public_daily_cache
import market_clock
import sina_daily
import threading
from collections import OrderedDict, defaultdict, deque
from contextlib import contextmanager

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
MODEL = "deepseek-flash"


def current_key():
    return config.get_key() or DEEPSEEK_API_KEY

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
_orig_request = requests.sessions.Session.request
_REQUEST_TIMEOUT_SCOPE = threading.local()


@contextmanager
def _default_http_timeout(seconds):
    """Supply a default only to requests made by this provider call's thread."""
    previous = getattr(_REQUEST_TIMEOUT_SCOPE, "seconds", None)
    _REQUEST_TIMEOUT_SCOPE.seconds = seconds
    try:
        yield
    finally:
        if previous is None:
            del _REQUEST_TIMEOUT_SCOPE.seconds
        else:
            _REQUEST_TIMEOUT_SCOPE.seconds = previous


def _patched_request(self, method, url, **kwargs):
    scoped_timeout = getattr(_REQUEST_TIMEOUT_SCOPE, "seconds", None)
    if scoped_timeout is not None and "timeout" not in kwargs:
        kwargs["timeout"] = scoped_timeout
    headers = dict(kwargs.get("headers") or {})
    headers.setdefault("User-Agent", _UA)
    if "eastmoney" in str(url):
        headers.setdefault("Referer", "https://quote.eastmoney.com/")
    kwargs["headers"] = headers
    return _orig_request(self, method, url, **kwargs)


requests.sessions.Session.request = _patched_request

_NAME_MAP = None
_NAME_MAP_AS_OF = None
_NAME_MAP_TTL_SECONDS = 7 * 86400
_NAME_MAP_LOCK = threading.Lock()
_NAME_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "name_map.json")


def _name_map_local():
    """Read public names without waiting for the provider or its refresh lock.

    Return (names, cache_timestamp, fresh). Callers must not present a stale
    cache as a current full-market universe.
    """
    names = _NAME_MAP if isinstance(_NAME_MAP, dict) and _NAME_MAP else None
    as_of = _NAME_MAP_AS_OF if names is not None else None
    try:
        file_as_of = os.path.getmtime(_NAME_CACHE_FILE)
        if as_of is None or file_as_of > as_of:
            with open(_NAME_CACHE_FILE, encoding="utf-8") as handle:
                saved = json.load(handle)
            if isinstance(saved, dict) and saved:
                names, as_of = saved, file_as_of
    except (OSError, ValueError, TypeError):
        pass
    now = time.time()
    fresh = bool(names and as_of is not None and 0 <= now - as_of < _NAME_MAP_TTL_SECONDS)
    return (dict(names) if names else {}), as_of, fresh


def _retry(fn, retries=3):
    for _ in range(retries):
        try:
            return fn()
        except requests.exceptions.RequestException:
            time.sleep(1)
    return None


def _num(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return None if pd.isna(v) else float(v)
    s = str(v).strip().replace(",", "")
    mul = 1.0
    if "万亿" in s:
        mul = 1e12
    elif "亿" in s:
        mul = 1e8
    elif "万" in s:
        mul = 1e4
    s = s.replace("万亿", "").replace("亿", "").replace("万", "").replace("%", "").strip()
    try:
        return float(s) * mul
    except ValueError:
        return None


def _clamp(x, lo=0, hi=100):
    return max(lo, min(hi, x))


def _prefix(code):
    if str(code).startswith("920"):
        return "bj"
    if code[0] in "69":
        return "sh"
    if code[0] in "48":
        return "bj"
    return "sz"


def _name_map():
    global _NAME_MAP, _NAME_MAP_AS_OF
    now = time.time()
    if (_NAME_MAP is not None and _NAME_MAP_AS_OF is not None
            and 0 <= now - _NAME_MAP_AS_OF < _NAME_MAP_TTL_SECONDS):
        return _NAME_MAP
    with _NAME_MAP_LOCK:
        now = time.time()
        if (_NAME_MAP is not None and _NAME_MAP_AS_OF is not None
                and 0 <= now - _NAME_MAP_AS_OF < _NAME_MAP_TTL_SECONDS):
            return _NAME_MAP
        # Concurrent homepage requests should share one public list fetch.
        try:
            if os.path.isfile(_NAME_CACHE_FILE):
                cache_as_of = os.path.getmtime(_NAME_CACHE_FILE)
            else:
                cache_as_of = None
            if cache_as_of is not None and 0 <= now - cache_as_of < _NAME_MAP_TTL_SECONDS:
                with open(_NAME_CACHE_FILE, encoding="utf-8") as f:
                    _NAME_MAP = json.load(f)
                if isinstance(_NAME_MAP, dict) and _NAME_MAP:
                    _NAME_MAP_AS_OF = cache_as_of
                    return _NAME_MAP
        except Exception:
            _NAME_MAP = None
            _NAME_MAP_AS_OF = None
        # AKShare 1.19.1 memoizes both the aggregate and each exchange list.
        # Refreshing our TTL must fetch new public inputs, not redate that cache.
        for name in ("stock_info_a_code_name", "stock_info_sh_name_code",
                     "stock_info_sz_name_code", "stock_info_bj_name_code"):
            clear = getattr(getattr(ak, name, None), "cache_clear", None)
            if callable(clear):
                clear()
        with _default_http_timeout(15):
            df = ak.stock_info_a_code_name()
        names = dict(zip(df["code"].astype(str), df["name"].astype(str)))
        if not names:
            raise ValueError("股票名称映射为空")
        _NAME_MAP = names
        _NAME_MAP_AS_OF = time.time()
        try:
            with open(_NAME_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(_NAME_MAP, f, ensure_ascii=False)
        except Exception:
            pass
        return _NAME_MAP


def get_stock_name(code):
    try:
        name = _name_map().get(str(code))
        if name:
            return name
    except Exception:
        pass
    try:
        info = _retry(lambda: ak.stock_individual_info_em(symbol=code))
        if info is not None and not info.empty:
            d = dict(zip(info["item"].astype(str), info["value"]))
            if d.get("股票简称"):
                return str(d["股票简称"])
    except Exception:
        pass
    try:
        return _name_map().get(code)
    except Exception:
        return None


SYSTEM_PROMPT = "你是严谨的A股分析师，只能基于工具返回的真实数据与程序计算结果分析，禁止编造数据或臆测未来价位。工具返回中的 quant_score（量化评分）、backtest（含夏普/换手/回撤/自相关等指标）、factor_analysis（因子IC，即各因子与未来收益的历史秩相关）均由程序确定性计算，必须如实引用并说明其局限性（回测未计手续费滑点、IC只是历史样本不代表未来；Margin与权重集中度属组合层面指标，单股不适用）；extra 字段若包含公司公告、龙虎榜、股东、行业信息，请纳入基本面或风险分析，字段为空则说明数据暂缺、不要编造。你的读者包含大量炒股新手，请用大白话讲解：每个专业术语第一次出现时用一句人话解释（例如 MA5 就是最近5个交易日的平均价），专业数值仍保留在括号里供进阶用户查看；买卖建议务必写成'什么情况下可以买、什么情况下该卖或减仓、什么情况下先别动'的日常语言，不要只堆术语。按固定结构输出，语言简洁：1. 基本面概览：核心数据+简要评价；2. 技术面分析：结合均线、MACD、RSI、KDJ、BOLL、量能逐一说明当前信号，并说明支撑/压力位；3. 量化参考与买卖点位建议：引用量化评分、因子IC与回测结果，明确支撑位、压力位及买卖触发条件；4. 风险提示：说明分析局限性，不构成投资建议。"

REVIEWER_PROMPT = "你是资深风控与证券分析复核人。审阅下面的个股分析草稿：1) 检查是否存在数据误用、夸大、自相矛盾，或与所给数据不符之处；2) 检查买卖建议是否过于绝对、风险提示是否充分；3) 确保每个专业术语都有大白话解释、买卖建议通俗易懂，让炒股新手也能看懂；4) 输出修正后的最终版，仍保持四段结构（基本面概览 / 技术面分析 / 量化参考与买卖点位建议 / 风险提示），语言简洁。若草稿无明显问题，可直接复述草稿并补充必要风险提示。"

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_stock_data",
            "description": "获取近60交易日K线、基本面（估值/成长/盈利质量/资金）、量化评分与历史回测结果",
            "parameters": {
                "type": "object",
                "properties": {
                    "stock_code": {"type": "string", "description": "6位股票代码，如002896"}
                },
                "required": ["stock_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "calc_technical_indicators",
            "description": "基于K线计算MA/MACD/RSI/KDJ/BOLL/ATR/量能、支撑压力位及因子评分",
            "parameters": {
                "type": "object",
                "properties": {
                    "kline_data": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "get_stock_data返回的kline数组",
                    }
                },
                "required": ["kline_data"],
            },
        },
    },
]


def _calc_yoy(series):
    if series is None:
        return None
    pairs = []
    for col, val in series.items():
        try:
            t = pd.Timestamp(str(col))
        except Exception:
            continue
        v = _num(val)
        if v is not None and v != 0:
            pairs.append((t, v))
    if not pairs:
        return None
    pairs.sort(key=lambda x: x[0], reverse=True)
    latest_t, latest_v = pairs[0]
    target = latest_t - pd.DateOffset(years=1)
    prev = next((v for t, v in pairs if t.year == target.year and t.month == target.month), None)
    if prev in (None, 0):
        return None
    return round((latest_v - prev) / abs(prev) * 100, 2)


def _latest_num(series):
    if series is None:
        return None
    for v in series:
        n = _num(v)
        if n is not None:
            return n
    return None


def _fundamentals(code):
    f = {"pe_ttm": None, "pe_percentile": None, "pb": None, "total_mv": None, "circ_mv": None,
         "rev_yoy": None, "profit_yoy": None, "roe": None, "gross_margin": None, "debt_ratio": None,
         "main_net_inflow_5d": None}
    try:
        info = _retry(lambda: ak.stock_individual_info_em(symbol=code))
        if info is not None and not info.empty:
            d = dict(zip(info["item"].astype(str), info["value"]))
            f["total_mv"] = _num(d.get("总市值"))
            f["circ_mv"] = _num(d.get("流通市值"))
    except Exception:
        pass
    try:
        pe = _retry(lambda: ak.stock_zh_valuation_baidu(symbol=code, indicator="市盈率(TTM)", period="全部"))
        if pe is not None and not pe.empty:
            vals = pd.to_numeric(pe["value"], errors="coerce").dropna()
            f["pe_ttm"] = round(float(vals.iloc[-1]), 2)
            f["pe_percentile"] = round(float((vals < vals.iloc[-1]).mean() * 100), 1)
    except Exception:
        pass
    if f["total_mv"] is None:
        try:
            mv = _retry(lambda: ak.stock_zh_valuation_baidu(symbol=code, indicator="总市值", period="近一年"))
            if mv is not None:
                mv = mv.dropna(subset=["value"])
            if mv is not None and not mv.empty:
                f["total_mv"] = round(float(mv.iloc[-1]["value"]) * 1e8, 2)
        except Exception:
            pass
    try:
        pb = _retry(lambda: ak.stock_zh_valuation_baidu(symbol=code, indicator="市净率", period="全部"))
        if pb is not None:
            pb = pb.dropna(subset=["value"])
        if pb is not None and not pb.empty:
            f["pb"] = round(float(pb.iloc[-1]["value"]), 2)
    except Exception:
        pass
    try:
        fa = _retry(lambda: ak.stock_financial_abstract(symbol=code))
        if fa is not None and "指标" in fa.columns:
            def _row(name):
                m = fa[fa["指标"] == name]
                return None if m.empty else m.iloc[0].drop(labels=["选项", "指标"], errors="ignore")

            f["rev_yoy"] = _calc_yoy(_row("营业总收入"))
            f["profit_yoy"] = _calc_yoy(_row("归母净利润"))
            f["roe"] = _latest_num(_row("净资产收益率(ROE)"))
            f["gross_margin"] = _latest_num(_row("毛利率"))
            f["debt_ratio"] = _latest_num(_row("资产负债率"))
    except Exception:
        pass
    try:
        ff = _retry(lambda: ak.stock_individual_fund_flow(stock=code, market=_prefix(code)))
        if ff is not None and not ff.empty and "主力净流入-净额" in ff.columns:
            s = pd.to_numeric(ff["主力净流入-净额"], errors="coerce").dropna()
            if len(s):
                f["main_net_inflow_5d"] = round(float(s.tail(5).sum()), 0)
    except Exception:
        pass
    return f


def _extra(code):
    """公司公告(巨潮)、龙虎榜(东财数据中心)、行业/十大股东(东财，限流时置空)"""
    extra = {"notices": [], "lhb": [], "industry": None, "holders": None}
    try:  # 近3个自然日的公司公告，按代码过滤
        today = pd.Timestamp.today()
        notices = []
        for i in range(3):
            day = (today - pd.Timedelta(days=i)).strftime("%Y%m%d")
            df = _retry(lambda d=day: ak.stock_notice_report(symbol="全部", date=d))
            if df is None or df.empty or "代码" not in df.columns:
                continue
            for _, r in df[df["代码"] == code].iterrows():
                notices.append({
                    "date": str(r.get("公告日期", "")),
                    "title": str(r.get("公告标题", "")),
                    "type": str(r.get("公告类型", "")),
                    "url": str(r.get("网址", "")),
                })
        extra["notices"] = notices[:10]
    except Exception:
        pass
    try:  # 近30天龙虎榜上榜记录
        end = pd.Timestamp.today().strftime("%Y%m%d")
        start = (pd.Timestamp.today() - pd.Timedelta(days=30)).strftime("%Y%m%d")
        df = _retry(lambda: ak.stock_lhb_detail_em(start_date=start, end_date=end))
        if df is not None and not df.empty and "代码" in df.columns:
            items = []
            for _, r in df[df["代码"] == code].iterrows():
                items.append({
                    "date": str(r.get("上榜日", "")),
                    "reason": str(r.get("上榜原因", "")),
                    "change_pct": _num(r.get("涨跌幅")),
                })
            extra["lhb"] = items[:10]
    except Exception:
        pass
    try:  # 行业（东财个股信息）
        info = _retry(lambda: ak.stock_individual_info_em(symbol=code))
        if info is not None and not info.empty:
            d = dict(zip(info["item"].astype(str), info["value"]))
            extra["industry"] = d.get("行业")
    except Exception:
        pass
    try:  # 十大流通股东（取最近一期可得季报）
        for q in ("20260630", "20260331", "20251231"):
            df = _retry(lambda q=q: ak.stock_gdfx_free_top_10_em(symbol=code, date=q))
            if df is not None and not df.empty:
                holders = []
                for _, r in df.head(10).iterrows():
                    holders.append({"name": str(r.get("股东名称", "")), "ratio": _num(r.get("占流通股比例"))})
                extra["holders"] = {"report_date": q, "holders": holders}
                break
    except Exception:
        pass
    return extra


_COLS = ["date", "open", "close", "high", "low", "volume"]
FAST_REQUEST_TIMEOUT_SECONDS = 5.0
FAST_SOURCE_RETRIES = 3
_FAST_SOURCE_SAMPLES = defaultdict(lambda: deque(maxlen=200))
_FAST_SOURCE_TOTALS = defaultdict(int)
_FAST_SOURCE_METRICS_LOCK = threading.Lock()
_FAST_CACHE_HITS = {"exact_success": 0, "exact_failure": 0, "covering_success": 0,
                    "singleflight_wait": 0, "singleflight_wait_timeout": 0}
_ANALYSIS_PRICE_CONTEXT = threading.local()
_FAST_KLINE_CACHE = OrderedDict()
_FAST_KLINE_COVERING = OrderedDict()
_FAST_KLINE_INFLIGHT = {}
_FAST_KLINE_LOCK = threading.Lock()
_FAST_KLINE_EXACT_LIMIT = 256
_FAST_KLINE_COVERING_LIMIT = 128
_FAST_KLINE_INFLIGHT_WAIT_SECONDS = 60
_USER_KLINE_CACHE = OrderedDict()
_USER_KLINE_LIMIT = 32
_USER_KLINE_MAX_BARS = 3000


def _trim_fast_kline_cache(now):
    """Called with _FAST_KLINE_LOCK held; bound retained quote frames after scans."""
    for key, (fetched_at, frame) in list(_FAST_KLINE_CACHE.items()):
        if now - fetched_at >= (300 if frame is not None else 20):
            del _FAST_KLINE_CACHE[key]
    for code, (fetched_at, *_rest) in list(_FAST_KLINE_COVERING.items()):
        if now - fetched_at >= 300:
            del _FAST_KLINE_COVERING[code]
    while len(_FAST_KLINE_CACHE) > _FAST_KLINE_EXACT_LIMIT:
        _FAST_KLINE_CACHE.popitem(last=False)
    while len(_FAST_KLINE_COVERING) > _FAST_KLINE_COVERING_LIMIT:
        _FAST_KLINE_COVERING.popitem(last=False)


def _kline_cached(code, start, end):
    # Reuse the bounded, dated public cache rather than an all-day immortal frame.
    return _kline_fast(code, start, end)


def _kline_em(code, start, end, timeout=FAST_REQUEST_TIMEOUT_SECONDS):
    df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start, end_date=end,
                            adjust="qfq", timeout=timeout)
    df = df.rename(columns={"日期": "date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low", "成交量": "volume"})[_COLS].copy()
    # AKShare documents this Eastmoney field in lots; Tencent and Sina return shares.
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce") * 100
    return df


def _tencent_daily_frame(symbol, start_date, end_date, timeout=FAST_REQUEST_TIMEOUT_SECONDS, adjustment="qfq"):
    """Read the same public qfq endpoint in nonoverlapping two-year spans.

    AKShare 1.19.1 requests these two-year spans once per year after an
    unbounded listing-date lookup. Neither overlap nor that lookup is needed
    for the requested range. Preserve its input units for the guard below.
    """
    start_day, end_day = pd.Timestamp(str(start_date)), pd.Timestamp(str(end_date))
    if adjustment not in ("qfq", ""):
        raise ValueError("Tencent adjustment must be qfq or raw")
    if start_day > end_day:
        raise ValueError("Tencent start date follows end date")
    wait = FAST_REQUEST_TIMEOUT_SECONDS if timeout is None else float(timeout)
    if wait <= 0 or not pd.notna(wait) or wait == float("inf"):
        raise ValueError("Tencent timeout must be finite and positive")
    end_year = min(end_day.year, datetime.now().year)
    rows = []
    request_count = 0
    selected_fields = set()
    for year in range(start_day.year, end_year + 1, 2):
        response = requests.get(
            "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get",
            params={"_var": f"kline_day{adjustment}{year}",
                    "param": f"{symbol},day,{year}-01-01,{year + 1}-12-31,640,{adjustment}",
                    "r": "0.8205512681390605"}, timeout=wait)
        request_count += 1
        response.raise_for_status()
        body = response.text
        prefix = body.find("={")
        payload = json.loads(body[prefix + 1:] if prefix >= 0 else body)
        data = payload.get("data", {}).get(symbol)
        if not isinstance(data, dict):
            raise ValueError("Tencent daily response lacks symbol data")
        field = next((name for name in (("qfqday", "day") if adjustment else ("day",)) if name in data), None)
        records = data.get(field) if field else None
        if not isinstance(records, list):
            raise ValueError("Tencent daily response lacks bar list")
        selected_fields.add(field)
        for row in records:
            if not isinstance(row, list) or len(row) < 9:
                raise ValueError("Tencent daily bar schema changed")
            rows.append([row[0], row[1], row[2], row[3], row[4], row[5], row[8]])
    frame = pd.DataFrame(rows, columns=[*_COLS, "amount"])
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    for column in (*_COLS[1:], "amount"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if not symbol.startswith(("sh688", "sh689", "sz399", "sh000", "sz000")):
        frame["volume"] *= 100
    frame["amount"] *= 10000
    frame = (frame.drop_duplicates().sort_values("date")
             .loc[lambda value: value["date"].between(start_day, end_day)]
             .reset_index(drop=True))
    frame.attrs["provider_http_requests"] = request_count
    frame.attrs["provider_price_fields"] = sorted(selected_fields)
    if symbol.startswith("sz000"):
        frame.attrs["volume_input_evidence"] = "tencent_raw_lots"
    return frame


def _kline_tx(code, start, end, timeout=FAST_REQUEST_TIMEOUT_SECONDS):
    df = _tencent_daily_frame(symbol=_prefix(code) + code, start_date=start, end_date=end,
                              timeout=timeout)
    df = df.copy()
    if not df.empty:
        # Guard against AKShare 1.19.1 skipping lots->shares for sz000 stocks.
        # Amount is in yuan; amount / (volume * close) is near 100 for lots
        # and near 1 for shares on a recent bar. Reject ambiguous units.
        latest = df.iloc[-1]
        try:
            amount = float(latest["amount"])
            volume = float(latest["volume"])
            close = float(latest["close"])
            trade_day = pd.Timestamp(latest["date"]).date()
            ratio = amount / (volume * close)
        except (KeyError, TypeError, ValueError, ZeroDivisionError, OverflowError) as error:
            raise ValueError("Tencent volume unit evidence missing") from error
        age_days = (datetime.now().date() - trade_day).days
        known_sz000_bug = (str(code).startswith("000")
                           and (getattr(ak, "__version__", None) == "1.19.1"
                                or df.attrs.get("volume_input_evidence") == "tencent_raw_lots"))
        if 0.25 <= ratio <= 4:
            df.attrs["volume_conversion"] = "already_shares"
        elif 25 <= ratio <= 400 and (0 <= age_days <= 35 or known_sz000_bug):
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce") * 100
            df.attrs["volume_conversion"] = "lots_to_shares"
        else:
            raise ValueError("Tencent volume unit ambiguous")
    df = df[_COLS].copy()
    return df


def _kline_sina(code, start, end):
    return sina_daily.daily(_prefix(code)+code,start,end,timeout=FAST_REQUEST_TIMEOUT_SECONDS)


def _kline(code, start, end):
    for fn in (_kline_em, _kline_tx, _kline_sina):
        df = _retry(lambda f=fn: f(code, start, end))
        if df is not None and not df.empty:
            return df
    return None


def _quant_score(tf, fund):
    pe_pct = fund.get("pe_percentile")
    val_score = (100 - pe_pct) if pe_pct is not None else 50
    rev, prof = fund.get("rev_yoy"), fund.get("profit_yoy")
    ys = [x for x in (rev, prof) if x is not None]
    growth_score = _clamp(50 + (sum(ys) / len(ys)) * 1.5, 0, 100) if ys else 50
    inflow = fund.get("main_net_inflow_5d")
    cap_score = 70 if inflow is not None and inflow > 0 else (30 if inflow is not None and inflow < 0 else 50)
    total = (tf["trend_score"] * 0.25 + tf["momentum_score"] * 0.25 + val_score * 0.15
             + growth_score * 0.15 + cap_score * 0.10 + tf["volatility_score"] * 0.10)
    total = round(total, 1)
    label = ("积极偏多" if total >= 70 else "谨慎偏多" if total >= 55 else "中性" if total >= 45
             else "谨慎偏空" if total >= 30 else "偏空")
    return {
        "total": total,
        "label": label,
        "factors": {
            "trend": round(tf["trend_score"], 1),
            "momentum": round(tf["momentum_score"], 1),
            "valuation": round(val_score, 1),
            "growth": round(growth_score, 1),
            "capital": round(cap_score, 1),
            "volatility": round(tf["volatility_score"], 1),
        },
    }


def _factor_ic(df, horizons=(5, 10, 20)):
    """因子IC：价量因子值与未来N日收益的秩相关（历史样本，仅供参考）"""
    d = df.copy()
    for c in ("close", "high", "low", "volume"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    close = d["close"]
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    rsi = 100 - 100 / (1 + gain / loss.replace(0, 1e-12))
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    macd = (dif - dea) * 2
    vol5 = d["volume"].rolling(5).mean()
    factors = {
        "趋势(价/MA20-1)": close / close.rolling(20).mean() - 1,
        "动量20日": close.pct_change(20),
        "动量60日": close.pct_change(60),
        "RSI14": rsi,
        "MACD柱": macd,
        "量比5日-1": d["volume"] / vol5 - 1,
    }
    rows = []
    for name, f in factors.items():
        row = {"factor": name}
        for h in horizons:
            fwd = close.shift(-h) / close - 1
            tmp = pd.DataFrame({"f": f, "r": fwd}).dropna()
            ic = tmp["f"].corr(tmp["r"], method="spearman") if len(tmp) >= 20 else None
            row[f"IC_{h}日"] = round(float(ic), 3) if ic is not None and pd.notna(ic) else None
        rows.append(row)
    return {"horizons": list(horizons), "factors": rows,
            "note": "IC=因子值与未来N日收益的秩相关；绝对值越大且方向越稳定，历史预测性越强，但不代表未来，且仅覆盖价量类因子"}


def _backtest(df):
    """MA5/20 金叉买、死叉卖 + 严格回测指标（未计手续费滑点，仅作参考）"""
    close = pd.to_numeric(df["close"], errors="coerce").reset_index(drop=True)
    ma5 = close.rolling(5).mean()
    ma20 = close.rolling(20).mean()
    pos, entry, rets, eq, daily = 0, 0.0, [], 1.0, []
    curve = [1.0]
    for i in range(len(close)):
        r = close.iloc[i] / close.iloc[i - 1] - 1 if i > 0 else 0.0
        strat_r = r if pos == 1 else 0.0
        daily.append(strat_r)
        eq *= 1 + strat_r
        curve.append(eq)
        if i > 0 and pd.notna(ma5.iloc[i]) and pd.notna(ma20.iloc[i]):
            golden = ma5.iloc[i - 1] <= ma20.iloc[i - 1] and ma5.iloc[i] > ma20.iloc[i]
            death = ma5.iloc[i - 1] >= ma20.iloc[i - 1] and ma5.iloc[i] < ma20.iloc[i]
            if pos == 0 and golden:
                pos, entry = 1, close.iloc[i]
            elif pos == 1 and death:
                pos = 0
                rets.append(close.iloc[i] / entry - 1)
    if pos == 1 and entry:
        rets.append(close.iloc[-1] / entry - 1)
    wins = sum(1 for r in rets if r > 0)
    s = pd.Series(curve)
    mdd = ((s.cummax() - s) / s.cummax()).max() * 100 if len(s) else 0
    ds = pd.Series(daily)
    n = len(ds)
    ann_ret = ((eq ** (252 / n)) - 1) * 100 if n and eq > 0 else 0.0
    vol = ds.std() * (252 ** 0.5) * 100 if n else 0.0
    sharpe = (ds.mean() * 252) / (ds.std() * (252 ** 0.5)) if n and ds.std() > 0 else 0.0
    auto = ds.autocorr(lag=1) if n > 3 else None
    turnover = len(rets) / (n / 252) if n else 0.0
    return {
        "trades": len(rets),
        "win_rate_pct": round(wins / len(rets) * 100, 1) if rets else 0,
        "strategy_return_pct": round((eq - 1) * 100, 2),
        "annualized_return_pct": round(ann_ret, 2),
        "buy_hold_return_pct": round((close.iloc[-1] / close.iloc[0] - 1) * 100, 2) if len(close) > 1 else 0,
        "max_drawdown_pct": round(mdd, 2),
        "sharpe": round(sharpe, 2),
        "annualized_volatility_pct": round(vol, 2),
        "turnover_per_year": round(turnover, 2),
        "return_autocorr": round(float(auto), 3) if auto is not None and pd.notna(auto) else None,
        "note": "单股简化回测(MA5/20金叉买死叉卖，未计手续费滑点)；Margin(边际贡献)与权重集中度属组合层面指标，单股不适用",
    }


def get_stock_data(stock_code):
    code = str(stock_code).strip()
    current=market_clock.now();today=current.date()
    end = current.strftime("%Y%m%d")
    start = (current - pd.Timedelta(days=400)).strftime("%Y%m%d")
    raw = _kline_cached(code, start, end)
    if raw is None or raw.empty:
        raise ValueError(f"股票代码 {code} 无效或暂无行情数据")
    import signal_snapshot
    df,rejected=signal_snapshot.clean_frame(raw,today)
    if df is None or df.empty:raise ValueError('没有已结束日线，不能生成已收盘技术分析')
    daily=market_clock.daily_evidence(df.iloc[-1]['date'],raw.attrs.get('source_retrieved_at'),current)
    if daily['kind'] in ('nontrading_day','future','unknown'):
        raise ValueError('最新历史日线日期不能核实为交易日，停止分析')
    import holdings
    quote=holdings.public_quote(code)
    context={'market_time':market_clock.snapshot(),'latest_quote':quote,'rule_daily_reference':daily,
        'latest_raw_daily':market_clock.daily_evidence(raw.iloc[-1]['date'],raw.attrs.get('source_retrieved_at')),
        'today_or_future_daily_excluded':int((pd.to_datetime(raw['date']).dt.date>=today).sum()),
        'invalid_daily_rejected':rejected}
    _ANALYSIS_PRICE_CONTEXT.value=context
    kline = [
        {
            "date": str(r["date"]),
            "open": round(float(r["open"]), 3),
            "close": round(float(r["close"]), 3),
            "high": round(float(r["high"]), 3),
            "low": round(float(r["low"]), 3),
            "volume": float(r["volume"]),
        }
        for _, r in df.tail(60).iterrows()
    ]
    tech = calc_technical_indicators(kline)
    fund = _fundamentals(code)
    name = get_stock_name(code) or code
    return {
        "stock_code": code,
        "stock_name": name,
        "price_time_context": context,
        "kline": kline,
        "fundamentals": fund,
        "quant_score": _quant_score(tech["factors"], fund),
        "backtest": _backtest(df),
        "factor_analysis": _factor_ic(df),
        "extra": _extra(code),
    }


def get_chart_data(code):
    """供图表使用：返回近一年完整K线（优先命中缓存）"""
    code = str(code).strip()
    current=market_clock.now()
    end = current.strftime("%Y%m%d")
    start = (current - pd.Timedelta(days=400)).strftime("%Y%m%d")
    df = _kline_cached(code, start, end)
    if df is None or df.empty:return None
    dates=pd.to_datetime(df['date'],errors='coerce')
    completed=df.loc[dates.notna() & dates.dt.date.lt(current.date())].copy()
    return None if completed.empty else completed


DEFAULT_POOL = [
    "600519", "000858", "601318", "600036", "000333", "002415",
    "300750", "002594", "601012", "600030", "000001", "600276",
    "603259", "002475", "600900", "601899", "300059", "002714",
]


def _fast_source_attempt(source, fn):
    """Retry provider failures while retaining code-free per-call evidence."""
    for attempt in range(FAST_SOURCE_RETRIES):
        started = time.monotonic()
        try:
            frame = fn()
            outcome = "usable" if frame is not None and not frame.empty else "empty"
        except requests.exceptions.Timeout:
            outcome, frame = "timeout", None
        except requests.exceptions.RequestException:
            outcome, frame = "request_error", None
        except Exception:
            outcome, frame = "provider_error", None
        elapsed = round((time.monotonic() - started) * 1000, 1)
        key = (source, outcome)
        with _FAST_SOURCE_METRICS_LOCK:
            _FAST_SOURCE_TOTALS[key] += 1
            _FAST_SOURCE_SAMPLES[key].append(elapsed)
        if outcome == "usable":
            return frame
        if outcome == "empty":
            return None
        if attempt + 1 < FAST_SOURCE_RETRIES:
            time.sleep(1)
    return None


def fast_quote_diagnostics():
    """Public, aggregate evidence only; no security code, dates or error text."""
    def percentile(values, fraction):
        ordered = sorted(values)
        location = (len(ordered) - 1) * fraction
        low = int(location)
        high = min(low + 1, len(ordered) - 1)
        return round(ordered[low] + (ordered[high] - ordered[low]) * (location - low), 1)

    with _FAST_SOURCE_METRICS_LOCK:
        rows = [(key, _FAST_SOURCE_TOTALS[key], list(values))
                for key, values in _FAST_SOURCE_SAMPLES.items()]
    with _FAST_KLINE_LOCK:
        cache_hits = dict(_FAST_CACHE_HITS)
        user_cache={'records':len(_USER_KLINE_CACHE),'limit':_USER_KLINE_LIMIT,
                    'bars':sum(len(frame) for _,frame in _USER_KLINE_CACHE.values()),
                    'max_bars_per_record':_USER_KLINE_MAX_BARS,'max_original_age_seconds':300}
    import source_check
    return {"scope": "current_process_only",
            "metric": "provider_function_call_ms_includes_local_parse_not_full_http_route",
            "timeout_note": f"腾讯和东财接口参数、新浪线程内默认单次 HTTP 请求均配置{FAST_REQUEST_TIMEOUT_SECONDS:g}秒；重试、解析和整条请求无硬截止",
            "cache_hits": cache_hits,
            "interactive_public_frames":user_cache,
            "secondary_prefetch":source_check.prefetch_diagnostics(),
            "shared_public_cache": public_daily_cache.diagnostics(),
            "groups": [{"source": source, "outcome": outcome, "total_count": total,
                        "retained_count": len(values),
                        "p50_ms": percentile(values, 0.50),
                        "p95_ms": percentile(values, 0.95),
                        "max_ms": max(values)}
                       for (source, outcome), total, values in sorted(rows)]}


def _kline_fast(code, start, end):
    namespace = ("fast_bj920_sina_v224" if str(code).startswith("920") else
                 "fast_sh689_receipts_sina_v224" if str(code).startswith("689") else "fast_tx_sina_em_v224")
    return public_daily_cache.daily(code, start, end, lambda: _kline_fast_local(code, start, end), namespace=namespace)


def _kline_fast_for_user(code,start,end):
    """Small foreground cache protected from scan eviction; no new freshness.

    It stores public frames only. The deadline is derived from the ORIGINAL
    retrieval/generation times, so a late promotion cannot extend their TTL.
    Qualification and second-source comparison remain the callers' work.
    """
    key=(str(code),str(start),str(end));now=time.monotonic()
    with _FAST_KLINE_LOCK:
        for old,(until,_) in list(_USER_KLINE_CACHE.items()):
            if now>=until:_USER_KLINE_CACHE.pop(old)
        hit=_USER_KLINE_CACHE.get(key)
        if hit:
            _USER_KLINE_CACHE.move_to_end(key)
            _FAST_CACHE_HITS['interactive_success']=_FAST_CACHE_HITS.get('interactive_success',0)+1
            result=hit[1].copy();result.attrs['request_cache_state']='memory';return result
    frame=_kline_fast(code,start,end)
    if frame is None or frame.empty or len(frame)>_USER_KLINE_MAX_BARS:return frame
    try:
        instants=[datetime.fromisoformat(frame.attrs[name]) for name in ['source_retrieved_at','cache_generated_at']]
        if any(x.tzinfo is None for x in instants):return frame
        ages=[(market_clock.now()-x).total_seconds() for x in instants]
        if any(x<0 or x>=300 for x in ages):return frame
        remaining=300-max(ages)
    except (KeyError,ValueError,TypeError):return frame
    with _FAST_KLINE_LOCK:
        _USER_KLINE_CACHE[key]=(time.monotonic()+remaining,frame.copy())
        _USER_KLINE_CACHE.move_to_end(key)
        while len(_USER_KLINE_CACHE)>_USER_KLINE_LIMIT:
            _USER_KLINE_CACHE.popitem(last=False)
            _FAST_CACHE_HITS['interactive_evicted']=_FAST_CACHE_HITS.get('interactive_evicted',0)+1
    return frame


def _kline_fast_local(code, start, end):
    """排名用快速取数：腾讯 -> 新浪 -> 东财（跳过东财限流等待）"""
    key = (str(code), str(start), str(end))
    while True:
        now = time.monotonic()
        with _FAST_KLINE_LOCK:
            _trim_fast_kline_cache(now)
            hit = _FAST_KLINE_CACHE.get(key)
            if hit:
                _FAST_KLINE_CACHE.move_to_end(key)
                if hit[1] is None:
                    _FAST_CACHE_HITS["exact_failure"] += 1
                    return None
                _FAST_CACHE_HITS["exact_success"] += 1
                result = hit[1].copy()
                result.attrs["request_cache_state"] = "memory"
                return result
            covering = _FAST_KLINE_COVERING.get(key[0])
            if (covering and now - covering[0] < 300
                    and covering[1] <= key[1] and covering[2] >= key[2]):
                dates = pd.to_datetime(covering[3]["date"], errors="coerce")
                selected = covering[3].loc[
                    dates.between(pd.Timestamp(key[1]), pd.Timestamp(key[2]))].copy()
                if not selected.empty:
                    _FAST_CACHE_HITS["covering_success"] += 1
                    selected.attrs.update(covering[3].attrs)
                    _FAST_KLINE_COVERING.move_to_end(key[0])
                    _FAST_KLINE_CACHE[key] = (covering[0], selected)
                    _trim_fast_kline_cache(now)
                    result = selected.copy()
                    result.attrs["request_cache_state"] = "memory"
                    return result
            waiter = _FAST_KLINE_INFLIGHT.get(key)
            if waiter is None:
                leader = threading.Event()
                _FAST_KLINE_INFLIGHT[key] = leader
                break
            _FAST_CACHE_HITS["singleflight_wait"] += 1
        if not waiter.wait(_FAST_KLINE_INFLIGHT_WAIT_SECONDS):
            with _FAST_KLINE_LOCK:
                _FAST_CACHE_HITS["singleflight_wait_timeout"] += 1
            return None
    try:
        for source, fn in (("tencent", _kline_tx), ("sina", _kline_sina), ("eastmoney", _kline_em)):
            if source == "sina":
                call = lambda f=fn: f(code, start, end)
            else:
                call = lambda f=fn: f(code, start, end, timeout=FAST_REQUEST_TIMEOUT_SECONDS)
            df = _fast_source_attempt(source, call)
            if df is not None and not df.empty:
                df = df.copy()
                df.attrs["source"] = source
                df.attrs["interval"] = "1d"
                df.attrs["adjustment"] = "qfq"
                df.attrs["volume_unit"] = "depositary_receipts" if str(code).startswith("689") else "shares"
                df.attrs["source_retrieved_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
                fetched_at = time.monotonic()
                df.attrs["cache_generated_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
                df.attrs["request_cache_state"] = "provider"
                cached = df.copy()
                with _FAST_KLINE_LOCK:
                    _FAST_KLINE_CACHE[key] = (fetched_at, cached)
                    prior = _FAST_KLINE_COVERING.get(key[0])
                    if (prior is None or fetched_at - prior[0] >= 300
                            or (key[1] <= prior[1] and key[2] >= prior[2])):
                        _FAST_KLINE_COVERING[key[0]] = (fetched_at, key[1], key[2], cached)
                    _trim_fast_kline_cache(fetched_at)
                return df
        with _FAST_KLINE_LOCK:
            failed_at = time.monotonic()
            _FAST_KLINE_CACHE[key] = (failed_at, None)
            _trim_fast_kline_cache(failed_at)
        return None
    finally:
        with _FAST_KLINE_LOCK:
            _FAST_KLINE_INFLIGHT.pop(key, None)
            leader.set()


def score_stock(code, start, end):
    df = _kline_fast(code, start, end)
    if df is None or df.empty:
        return None
    kline = [
        {"date": str(r["date"]), "open": round(float(r["open"]), 3), "close": round(float(r["close"]), 3),
         "high": round(float(r["high"]), 3), "low": round(float(r["low"]), 3), "volume": float(r["volume"])}
        for _, r in df.tail(60).iterrows()
    ]
    tech = calc_technical_indicators(kline)
    f = tech["factors"]
    return {
        "code": code,
        "name": get_stock_name(code) or code,
        "close": tech["latest"]["close"],
        "trend": f["trend_score"],
        "momentum": f["momentum_score"],
        "volatility": f["volatility_score"],
        "score": round(f["trend_score"] * 0.4 + f["momentum_score"] * 0.4 + f["volatility_score"] * 0.2, 1),
    }


def _rank_fundamentals(code):
    """排名用轻量基本面：百度PE分位 + 财务摘要成长，跳过东财（快）"""
    out = {"pe_percentile": None, "rev_yoy": None, "profit_yoy": None}
    try:
        pe = _retry(lambda: ak.stock_zh_valuation_baidu(symbol=code, indicator="市盈率(TTM)", period="全部"))
        if pe is not None and not pe.empty:
            vals = pd.to_numeric(pe["value"], errors="coerce").dropna()
            out["pe_percentile"] = round(float((vals < vals.iloc[-1]).mean() * 100), 1)
    except Exception:
        pass
    try:
        fa = _retry(lambda: ak.stock_financial_abstract(symbol=code))
        if fa is not None and "指标" in fa.columns:
            def _row(name):
                m = fa[fa["指标"] == name]
                return None if m.empty else m.iloc[0].drop(labels=["选项", "指标"], errors="ignore")

            out["rev_yoy"] = _calc_yoy(_row("营业总收入"))
            out["profit_yoy"] = _calc_yoy(_row("归母净利润"))
    except Exception:
        pass
    return out


def score_stock_full(code, start, end):
    """含基本面的打分：趋势30% 动量30% 波动10% 估值15% 成长15%"""
    df = _kline_fast(code, start, end)
    if df is None or df.empty:
        return None
    kline = [
        {"date": str(r["date"]), "open": round(float(r["open"]), 3), "close": round(float(r["close"]), 3),
         "high": round(float(r["high"]), 3), "low": round(float(r["low"]), 3), "volume": float(r["volume"])}
        for _, r in df.tail(60).iterrows()
    ]
    tech = calc_technical_indicators(kline)
    f = tech["factors"]
    fund = _rank_fundamentals(code)
    pe_pct = fund.get("pe_percentile")
    val = (100 - pe_pct) if pe_pct is not None else 50
    ys = [x for x in (fund.get("rev_yoy"), fund.get("profit_yoy")) if x is not None]
    growth = _clamp(50 + (sum(ys) / len(ys)) * 1.5, 0, 100) if ys else 50
    score = round(f["trend_score"] * 0.30 + f["momentum_score"] * 0.30 + f["volatility_score"] * 0.10
                  + val * 0.15 + growth * 0.15, 1)
    return {
        "code": code,
        "name": get_stock_name(code) or code,
        "close": tech["latest"]["close"],
        "trend": f["trend_score"],
        "momentum": f["momentum_score"],
        "volatility": f["volatility_score"],
        "valuation": round(val, 1),
        "growth": round(growth, 1),
        "score": score,
    }


def rank_pool(pool=None, with_fundamentals=True):
    """打分排名：含基本面（趋势30% 动量30% 波动10% 估值15% 成长15%），仅供研究"""
    pool = pool or DEFAULT_POOL
    end = pd.Timestamp.today().strftime("%Y%m%d")
    start = (pd.Timestamp.today() - pd.Timedelta(days=400)).strftime("%Y%m%d")
    rows = []
    fn = score_stock_full if with_fundamentals else score_stock
    for code in pool:
        try:
            r = fn(code, start, end)
            if r:
                rows.append(r)
        except Exception:
            continue
    rows.sort(key=lambda x: x["score"], reverse=True)
    for i, r in enumerate(rows):
        r["rank"] = i + 1
    return rows


def _r(x, n=3):
    try:
        x = float(x)
        return None if pd.isna(x) else round(x, n)
    except (TypeError, ValueError):
        return None


def calc_technical_indicators(kline_data):
    if isinstance(kline_data, str):
        kline_data = json.loads(kline_data)
    if isinstance(kline_data, dict):
        kline_data = kline_data.get("kline") or kline_data.get("kline_data") or []
    if not kline_data:
        return {"error": "K线数据为空"}
    df = pd.DataFrame(kline_data)
    close = pd.to_numeric(df["close"], errors="coerce").dropna().reset_index(drop=True)
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    volume = pd.to_numeric(df["volume"], errors="coerce")

    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    macd = (dif - dea) * 2

    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    rsi = 100 - 100 / (1 + gain / loss.replace(0, 1e-12))

    low_n = low.rolling(9).min()
    high_n = high.rolling(9).max()
    rsv = (close - low_n) / (high_n - low_n).replace(0, 1e-12) * 100
    k = rsv.ewm(com=2, adjust=False).mean()
    d = k.ewm(com=2, adjust=False).mean()
    j = 3 * k - 2 * d

    boll_mid = close.rolling(20).mean()
    boll_std = close.rolling(20).std()
    boll_up = boll_mid + 2 * boll_std
    boll_low = boll_mid - 2 * boll_std

    prev_close = close.shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()

    vol_ma5 = volume.rolling(5).mean()
    vol_ratio = volume.iloc[-1] / vol_ma5.iloc[-1] if pd.notna(vol_ma5.iloc[-1]) and vol_ma5.iloc[-1] else None

    c, hv, lv = close.iloc[-1], high.tail(20).max(), low.tail(20).min()
    support = round(lv, 2)
    resistance = round(hv, 2)

    ma_sig = "多头排列" if ma5.iloc[-1] > ma10.iloc[-1] > ma20.iloc[-1] else \
        ("空头排列" if ma5.iloc[-1] < ma10.iloc[-1] < ma20.iloc[-1] else "均线纠缠")
    if dif.iloc[-1] > dea.iloc[-1] and dif.iloc[-2] <= dea.iloc[-2]:
        macd_sig = "MACD金叉，短期偏多"
    elif dif.iloc[-1] < dea.iloc[-1] and dif.iloc[-2] >= dea.iloc[-2]:
        macd_sig = "MACD死叉，短期偏空"
    else:
        macd_sig = "MACD多头区间" if dif.iloc[-1] > dea.iloc[-1] else "MACD空头区间"
    r = rsi.iloc[-1]
    rsi_sig = "超买" if r > 70 else ("超卖" if r < 30 else ("偏强" if r > 50 else "偏弱"))
    kdj_sig = "KDJ金叉（K>D）" if k.iloc[-1] > d.iloc[-1] else "KDJ死叉（K<D）"
    boll_sig = "触及/站上上轨" if c >= boll_up.iloc[-1] else ("触及/跌破下轨" if c <= boll_low.iloc[-1] else "中轨附近运行")

    def _cond(a, b):
        if pd.isna(a) or pd.isna(b):
            return None
        return bool(a > b)

    conds = [_cond(c, ma5.iloc[-1]), _cond(c, ma10.iloc[-1]), _cond(c, ma20.iloc[-1]), _cond(c, ma60.iloc[-1]),
             _cond(ma5.iloc[-1], ma10.iloc[-1]), _cond(ma10.iloc[-1], ma20.iloc[-1]), _cond(ma20.iloc[-1], ma60.iloc[-1])]
    valid = [x for x in conds if x is not None]
    trend_score = round(sum(valid) / len(valid) * 100, 1) if valid else 50

    if dif.iloc[-1] > dea.iloc[-1] and macd.iloc[-1] > 0:
        macd_m = 80
    elif dif.iloc[-1] > dea.iloc[-1]:
        macd_m = 60
    elif dif.iloc[-1] < dea.iloc[-1] and macd.iloc[-1] < 0:
        macd_m = 20
    else:
        macd_m = 40
    if 45 <= r <= 65:
        rsi_m = 80
    elif (40 <= r < 45) or (65 < r <= 70):
        rsi_m = 60
    elif r > 70:
        rsi_m = 35
    elif r < 30:
        rsi_m = 30
    else:
        rsi_m = 50
    if k.iloc[-1] > d.iloc[-1] and j.iloc[-1] < 100:
        kdj_m = 75
    elif k.iloc[-1] > d.iloc[-1]:
        kdj_m = 60
    elif k.iloc[-1] < d.iloc[-1] and j.iloc[-1] < 0:
        kdj_m = 20
    elif k.iloc[-1] < d.iloc[-1]:
        kdj_m = 40
    else:
        kdj_m = 50
    momentum_score = round((macd_m + rsi_m + kdj_m) / 3, 1)

    atr_pct = atr.iloc[-1] / c * 100 if pd.notna(atr.iloc[-1]) and c else None
    if atr_pct is None:
        volatility_score = 50
    elif atr_pct < 2:
        volatility_score = 80
    elif atr_pct < 4:
        volatility_score = 60
    elif atr_pct < 6:
        volatility_score = 40
    elif atr_pct < 8:
        volatility_score = 25
    else:
        volatility_score = 15

    return {
        "latest": {
            "date": str(df["date"].iloc[-1]) if "date" in df.columns else None,
            "close": _r(c),
            "MA5": _r(ma5.iloc[-1]), "MA10": _r(ma10.iloc[-1]), "MA20": _r(ma20.iloc[-1]), "MA60": _r(ma60.iloc[-1]),
            "DIF": _r(dif.iloc[-1]), "DEA": _r(dea.iloc[-1]), "MACD": _r(macd.iloc[-1]), "RSI14": _r(r, 2),
            "K": _r(k.iloc[-1], 2), "D": _r(d.iloc[-1], 2), "J": _r(j.iloc[-1], 2),
            "BOLL_up": _r(boll_up.iloc[-1]), "BOLL_mid": _r(boll_mid.iloc[-1]), "BOLL_low": _r(boll_low.iloc[-1]),
            "ATR14": _r(atr.iloc[-1]), "ATR_pct": _r(atr_pct, 2), "vol_ratio": _r(vol_ratio, 2),
        },
        "signals": {"ma": ma_sig, "macd": macd_sig, "rsi": f"RSI14={r:.2f}，{rsi_sig}",
                    "kdj": kdj_sig, "boll": boll_sig,
                    "volume": f"量比={vol_ratio:.2f}" if vol_ratio is not None else "量能数据缺失"},
        "support_resistance": {"support": support, "resistance": resistance},
        "factors": {"trend_score": trend_score, "momentum_score": momentum_score, "volatility_score": volatility_score},
    }


def _chat(messages, use_tools=True):
    payload = {"model": MODEL, "thinking":{"type":"disabled"}, "messages": messages, "temperature": 0.3}
    if use_tools:
        payload["tools"] = TOOLS
    resp = requests.post(
        DEEPSEEK_BASE_URL.rstrip("/") + "/chat/completions",
        headers={"Authorization": f"Bearer {current_key()}", "Content-Type": "application/json"},
        json=payload,
        timeout=60,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"DeepSeek API 请求失败: HTTP {resp.status_code}；请核对服务商与授权配置")
    return resp.json()["choices"][0]["message"]


def _dispatch(tool_call):
    name = tool_call["function"]["name"]
    args = json.loads(tool_call["function"].get("arguments") or "{}")
    if name == "get_stock_data":
        return json.dumps(get_stock_data(args["stock_code"]), ensure_ascii=False)
    if name == "calc_technical_indicators":
        return json.dumps(calc_technical_indicators(args.get("kline_data")), ensure_ascii=False)
    return json.dumps({"error": f"未知工具 {name}"}, ensure_ascii=False)


def _run_tools(code):
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT+'\n'+market_clock.prompt_context()},
        {"role": "user", "content": f"请分析股票 {code}：先调用 get_stock_data 获取数据（含量化评分与回测），再调用 calc_technical_indicators 计算技术指标，最后给出完整分析。"},
    ]
    for _ in range(4):
        msg = _chat(messages)
        messages.append(msg)
        if msg.get("tool_calls"):
            for tc in msg["tool_calls"]:
                messages.append({"role": "tool", "tool_call_id": tc["id"], "content": _dispatch(tc)})
            continue
        return msg["content"]
    return "分析未完成：工具调用轮次超出限制"


def analyze(stock_code):
    _ANALYSIS_PRICE_CONTEXT.value=None
    draft = _run_tools(stock_code)
    context=getattr(_ANALYSIS_PRICE_CONTEXT,'value',None)
    rev = _chat([
        {"role": "system", "content": REVIEWER_PROMPT+'\n'+market_clock.prompt_context()},
        {"role": "user", "content": '必须按以下时间/价格证据纠正今日收盘、实时与休市表述：\n'+json.dumps(context,ensure_ascii=False)+'\n'+f"请复核以下分析并输出最终版：\n\n{draft}"},
    ], use_tools=False)
    state=market_clock.snapshot()
    quote=context.get('latest_quote',{}) if context else {}
    daily=context.get('rule_daily_reference',{}) if context else {}
    header=('> 上海时间 '+state['server_time']+' · '+state['weekday']+' · '+state['phase_label']+'。\n'
        '> 报价参考：'+str(quote.get('price') if quote.get('price') is not None else '未知')+'；源时刻 '+str(quote.get('source_time') or '未知')+'；'
        +str(quote.get('price_evidence',{}).get('label','价格类型未知'))+'。\n'
        '> 技术/规则日线：'+str(daily.get('label','来源日期未知'))+'；盘中报价不代替完整日线或假定成交。\n\n')
    return header+(rev.get("content") or draft)


if __name__ == "__main__":
    if not current_key():
        print("请先设置环境变量 DEEPSEEK_API_KEY 后重试")
        sys.exit(1)
    code = sys.argv[1] if len(sys.argv) > 1 else input("请输入6位股票代码：").strip()
    try:
        print(analyze(code))
    except Exception as e:
        print(f"分析失败：{e}")
