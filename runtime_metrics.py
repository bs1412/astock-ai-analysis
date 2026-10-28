# -*- coding: utf-8 -*-
"""进程内、无证券标识的请求处理耗时摘要。进程重启即清空。"""
from collections import defaultdict, deque
from datetime import datetime
import math
import threading

import stock_agent


_LOCK = threading.Lock()
_SAMPLES = defaultdict(lambda: deque(maxlen=200))
_TOTALS = defaultdict(int)
_ROUTES = frozenset(("holdings_quotes", "decision", "decision_replay", "watch_signal", "scan_start"))
_OUTCOMES = frozenset(("usable", "partial", "unavailable", "empty", "accepted", "busy", "error"))
_CACHES = frozenset(("memory", "provider", "unknown"))


def record(route, outcome, cache, elapsed_ms):
    if (route not in _ROUTES or outcome not in _OUTCOMES or cache not in _CACHES
            or not isinstance(elapsed_ms, (float, int)) or not math.isfinite(elapsed_ms)
            or elapsed_ms < 0):
        return
    key = (route, outcome, cache)
    with _LOCK:
        _TOTALS[key] += 1
        _SAMPLES[key].append(round(float(elapsed_ms), 1))


def _percentile(values, pct):
    ordered = sorted(values)
    index = (len(ordered) - 1) * pct
    lo = int(index)
    hi = min(lo + 1, len(ordered) - 1)
    return round(ordered[lo] + (ordered[hi] - ordered[lo]) * (index - lo), 1)


def snapshot():
    with _LOCK:
        groups = [(key, _TOTALS[key], list(values)) for key, values in _SAMPLES.items()]
    return {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "scope": "current_process_only",
        "metric": "server_processing_ms_excludes_client_render_and_response_transfer",
        "sample_limit_per_group": 200,
        "groups": [{"route": route, "outcome": outcome, "cache": cache,
                    "total_count": total, "retained_count": len(values),
                    "p50_ms": _percentile(values, 0.50),
                    "p95_ms": _percentile(values, 0.95)}
                   for (route, outcome, cache), total, values in sorted(groups)],
        "fast_quote_sources": stock_agent.fast_quote_diagnostics(),
        "caution": "仅为本进程分组处理耗时；缓存分组只标记主日线取数，第二来源核对与历史计算仍可能耗时。不能由此证明浏览器首屏、真实全市场或因果提速。",
    }
