# -*- coding: utf-8 -*-
"""On-demand second-source check for current daily quote decisions.

The caller waits at most six seconds for this check. A timed-out provider worker
may continue in the background; the limit does not cover primary data loading.
"""
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import threading
import time

import signal_snapshot
import stock_agent as sa
import public_daily_cache


SECONDARY_TIMEOUT_SECONDS = 5
SECONDARY_LOOKBACK_DAYS = 30
CHECK_CACHE_SECONDS = 300
FAILURE_CACHE_SECONDS = 60
CHECK_CACHE_LIMIT = 256
SECONDARY_WAIT_SECONDS = 6
MAX_ACTIVE_SECONDARY_CALLS = 8
PREFETCH_PRIMARY_RESERVE_SECONDS = 30

_CHECK_CACHE = OrderedDict()
_INFLIGHT = {}
_LOCK = threading.Lock()
_WORKER_LIMIT = threading.BoundedSemaphore(MAX_ACTIVE_SECONDARY_CALLS)
_PREFETCH_POOL=ThreadPoolExecutor(max_workers=4,thread_name_prefix='sina-input')
_PREFETCH_LIMIT=threading.BoundedSemaphore(8)
_PREFETCH_LOCK=threading.Lock()
_PREFETCH=OrderedDict()
_PREFETCH_COUNTS={'started':0,'reused':0,'capacity_skipped':0}


def _secondary_input(source,fn,code,today,invoked=None,minimum_remaining_seconds=0):
    start=(today-timedelta(days=SECONDARY_LOOKBACK_DAYS)).strftime('%Y%m%d');end=today.strftime('%Y%m%d')
    def fetch():
        if invoked is not None:invoked.append(source)
        frame=fn(code,start,end) if source=='sina' else fn(code,start,end,timeout=SECONDARY_TIMEOUT_SECONDS)
        if frame is not None and not frame.empty:
            frame=frame.copy();stamp=datetime.now(signal_snapshot.SHANGHAI).isoformat(timespec='seconds')
            frame.attrs.update(source=source,interval='1d',adjustment='qfq',
                volume_unit='depositary_receipts' if str(code).startswith('689') else 'shares',
                source_retrieved_at=stamp,cache_generated_at=stamp)
        return frame
    version=('_bj920_sina_v224' if str(code).startswith('920') else
             '_sh689_receipts_sina_v224' if str(code).startswith('689') else '_v224')
    return public_daily_cache.daily(code,start,end,fetch,namespace='secondary_'+source+version,
        minimum_remaining_seconds=minimum_remaining_seconds)


def _prefetch_current(entry,minimum_remaining_seconds=0):
    future,started=entry
    if not future.done():return True
    try:frame=future.result()
    except Exception:return time.monotonic()-started<FAILURE_CACHE_SECONDS
    if frame is None or frame.empty:return time.monotonic()-started<FAILURE_CACHE_SECONDS
    try:
        stamps=[datetime.fromisoformat(frame.attrs[n]) for n in ['source_retrieved_at','cache_generated_at']]
        ages=[(datetime.now(signal_snapshot.SHANGHAI)-stamp).total_seconds() for stamp in stamps]
        return all(stamp.tzinfo is not None for stamp in stamps) and all(0<=age<CHECK_CACHE_SECONDS-minimum_remaining_seconds for age in ages)
    except (ValueError,TypeError,KeyError):return False


def prefetch_sina(code,today,minimum_remaining_seconds=0):
    """Overlap public Sina input with primary loading, without granting a signal.

    Actual primary source is still chosen after loading. A Sina primary cannot
    compare with this input. Original retrieval times cap reuse, including a
    shared-cache hit; result comparison always binds the actual primary quote.
    """
    key=(str(code),today.isoformat())
    with _PREFETCH_LOCK:
        for old,entry in list(_PREFETCH.items()):
            if not _prefetch_current(entry):_PREFETCH.pop(old)
        entry=_PREFETCH.get(key)
        if entry and not _prefetch_current(entry,minimum_remaining_seconds):
            _PREFETCH.pop(key);entry=None
        if entry:
            _PREFETCH.move_to_end(key);_PREFETCH_COUNTS['reused']+=1;return entry[0]
        if not _PREFETCH_LIMIT.acquire(blocking=False):
            _PREFETCH_COUNTS['capacity_skipped']+=1;return None
        try:future=_PREFETCH_POOL.submit(_secondary_input,'sina',sa._kline_sina,code,today,
            minimum_remaining_seconds=minimum_remaining_seconds)
        except Exception:
            _PREFETCH_LIMIT.release();_PREFETCH_COUNTS['capacity_skipped']+=1;return None
        _PREFETCH[key]=(future,time.monotonic());_PREFETCH_COUNTS['started']+=1
        while len(_PREFETCH)>32:
            completed=next((old for old,entry in _PREFETCH.items() if entry[0].done()),None)
            if completed is None:break
            _PREFETCH.pop(completed)
    future.add_done_callback(lambda done:_PREFETCH_LIMIT.release())
    return future


def prefetch_diagnostics():
    with _PREFETCH_LOCK:return {**_PREFETCH_COUNTS,'records':len(_PREFETCH),'record_limit':32,'worker_limit':4,'pending_limit':8,
        'primary_load_reserve_seconds':PREFETCH_PRIMARY_RESERVE_SECONDS,
        'scope':'public_sina_inputs_only_comparison_and_signal_qualification_still_required'}


def _unavailable(snapshot_id, reason):
    return {"status": "unavailable", "sources": [], "trade_dates": [],
            "reasons": [reason], "close_diff_pct": None, "volume_diff_pct": None,
            "primary_snapshot_id": snapshot_id}


def _cached(key, now):
    hit = _CHECK_CACHE.get(key)
    if hit is None:
        return None
    at, result = hit
    lifetime = (FAILURE_CACHE_SECONDS if result["status"] == "unavailable"
                else CHECK_CACHE_SECONDS)
    if now - at >= lifetime:
        del _CHECK_CACHE[key]
        return None
    _CHECK_CACHE.move_to_end(key)
    return {**result, "cache_hit": True}


def _run_check(key, event, code, primary, today, snapshot_id):
    """Complete and cache one check even when its initial caller has timed out."""
    started = time.monotonic()
    network_calls = 0
    attempted_sources = []
    prefetched_sources=[]
    failures = []
    result = _unavailable(snapshot_id, "第二来源核对未完成")
    try:
        primary_source = primary.attrs.get("source")
        alternatives = {
            "tencent": (("sina", sa._kline_sina), ("eastmoney", sa._kline_em)),
            "sina": (("tencent", sa._kline_tx), ("eastmoney", sa._kline_em)),
            "eastmoney": (("tencent", sa._kline_tx), ("sina", sa._kline_sina)),
        }.get(primary_source)
        if alternatives is None:
            result = _unavailable(snapshot_id, "主源未知，无法选择独立第二来源")
        else:
            for source, fn in alternatives:
                attempted_sources.append(source)
                try:
                    future=prefetch_sina(code,today) if source=='sina' else None
                    if future is not None:
                        secondary=future.result();prefetched_sources.append(source)
                    else:
                        invocations=[];secondary=_secondary_input(source,fn,code,today,invocations)
                        network_calls+=len(invocations)
                except Exception as error:
                    failures.append(f"{source}取数失败：{type(error).__name__}")
                    continue
                if secondary is None or secondary.empty:
                    failures.append(f"{source}未返回可比较的日线")
                    continue
                secondary = secondary.copy()
                try:
                    result = signal_snapshot.compare_sources(primary, secondary, today, code=code)
                except Exception as error:
                    result = _unavailable(snapshot_id, f"{source}日线无法比较：{type(error).__name__}")
                break  # Never search for a more favorable comparison after a result.
            else:
                result = _unavailable(snapshot_id, "；".join(failures))
    except Exception as error:
        result = _unavailable(snapshot_id, f"第二来源核对内部失败：{type(error).__name__}")
    finally:
        result["attempted_sources"] = attempted_sources
        result["fallback_failures"] = failures
        result["checked_at"] = datetime.now(signal_snapshot.SHANGHAI).isoformat(timespec="seconds")
        result["elapsed_ms"] = round((time.monotonic() - started) * 1000)
        result["provider_invocations"] = network_calls
        result['prefetched_sources']=prefetched_sources
        result['invocation_scope']='direct_calls_inside_comparison_worker_excludes_earlier_prefetch'
        result["cache_hit"] = False
        try:
            with _LOCK:
                try:
                    _CHECK_CACHE[key] = (time.monotonic(), dict(result))
                    _CHECK_CACHE.move_to_end(key)
                    while len(_CHECK_CACHE) > CHECK_CACHE_LIMIT:
                        _CHECK_CACHE.popitem(last=False)
                finally:
                    _INFLIGHT.pop(key, None)
        finally:
            event.set()
            _WORKER_LIMIT.release()


def verify_secondary(code, primary, today):
    """Compare a quote with Eastmoney, bounding this check's caller wait."""
    snapshot_id = signal_snapshot.quote_snapshot_id(code, primary, today)
    if snapshot_id is None:
        return _unavailable(None, "主源没有可核对的已结束日线")
    key = (snapshot_id, today.isoformat())
    started = time.monotonic()
    with _LOCK:
        cached = _cached(key, started)
        if cached is not None:
            return cached
        event = _INFLIGHT.get(key)
        owner = event is None
        if owner:
            if not _WORKER_LIMIT.acquire(blocking=False):
                result = _unavailable(snapshot_id, "第二来源并发核对达到上限，请稍后重试")
                result.update({"checked_at": datetime.now(signal_snapshot.SHANGHAI).isoformat(timespec="seconds"),
                               "elapsed_ms": 0, "provider_invocations": 0, "cache_hit": False})
                return result
            event = threading.Event()
            _INFLIGHT[key] = event
    if owner:
        worker = threading.Thread(target=_run_check,
                                  args=(key, event, code, primary, today, snapshot_id),
                                  daemon=True, name="secondary-quote-check")
        try:
            worker.start()
        except Exception as error:
            with _LOCK:
                _INFLIGHT.pop(key, None)
                event.set()
            _WORKER_LIMIT.release()
            result = _unavailable(snapshot_id, f"第二来源核对未启动：{type(error).__name__}")
            result.update({"checked_at": datetime.now(signal_snapshot.SHANGHAI).isoformat(timespec="seconds"),
                           "elapsed_ms": round((time.monotonic() - started) * 1000),
                           "provider_invocations": 0, "cache_hit": False})
            return result
    event.wait(SECONDARY_WAIT_SECONDS)
    with _LOCK:
        cached = _cached(key, time.monotonic())
    if cached is not None:
        return {**cached, "cache_hit": not owner}
    result = _unavailable(snapshot_id, "第二来源核对超过本次等待上限，后台可能仍在进行，请稍后重试")
    result.update({"checked_at": datetime.now(signal_snapshot.SHANGHAI).isoformat(timespec="seconds"),
                   "elapsed_ms": round((time.monotonic() - started) * 1000),
                   "provider_invocations": None, "cache_hit": False})
    return result


def with_secondary_check(code, frame, today):
    """Return a copy carrying this exact quote's comparison, leaving cache frames alone."""
    if frame is None:
        return None
    checked = frame.copy()
    checked.attrs.update(frame.attrs)
    checked.attrs["source_comparison"] = verify_secondary(code, frame, today)
    return checked
