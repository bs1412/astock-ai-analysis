# -*- coding: utf-8 -*-
"""网页版A股分析：本地HTTP服务，多股连续分析 + 历史存档。"""
import json
import hashlib
import hmac
import ipaddress
import logging
import math
import os
import re
import socket
import threading
import copy
import time
from datetime import datetime, date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, urlsplit, unquote, parse_qs

import stock_agent
import charts
import portfolio
import holdings
import watchlist
import market
import config
import screener
import paper
import outlook
import entry_signals
import exit_signals
import decision
import runtime_metrics
import runtime_identity
import private_storage
import research_job
import market_clock
import brief_snapshot
from functools import wraps

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WEB_DIR = os.path.join(BASE_DIR, "web")
HISTORY_ROOT = os.path.join(BASE_DIR, "历史记录")
HOST = os.getenv("WEB_BIND_HOST", "127.0.0.1").strip() or "127.0.0.1"
PORT = int(os.getenv("WEB_PORT", "8000") or 8000)
ALLOWED_HOSTS = {item.strip().lower() for item in os.getenv("WEB_ALLOWED_HOSTS", "").split(",")
                 if item.strip()}
MAX_REQUEST_BYTES = 1024 * 1024
MAX_CONCURRENT = 3
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN", "").strip()
DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "50") or 50)
USAGE_FILE = os.path.join(BASE_DIR, "usage.json")

os.makedirs(HISTORY_ROOT, exist_ok=True)

log = logging.getLogger("web")
log.setLevel(logging.INFO)
log.propagate = False
if not log.handlers:
    _fh = logging.FileHandler(os.path.join(BASE_DIR, "server.log"), encoding="utf-8")
    _fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(_fh)
_sem = threading.Semaphore(MAX_CONCURRENT)
_PORTFOLIO_LOCK = threading.Lock()
_PORTFOLIO_JOB = {"running": False, "stage": "idle", "done": 0, "total": 0,
                  "message": "尚未运行", "result": None, "error": None,
                  "started_at": None, "finished_at": None, "pool": [],
                  "fee_pct": 0.1, "slippage_pct": 0.05}
_PAPER_LOCK = threading.Lock()
_PAPER_JOB = {"running": False, "stage": "idle", "done": 0, "total": 0,
              "message": "尚未运行", "result": None, "error": None,
              "started_at": None, "finished_at": None}
runtime_identity.initialize()


def _portfolio_progress(stage, done, total, message):
    with _PORTFOLIO_LOCK:
        _PORTFOLIO_JOB.update({"stage": stage, "done": done, "total": total, "message": message})


def _portfolio_worker(pool, fee_pct, slippage_pct):
    try:
        result = portfolio.portfolio_analysis(pool=pool, fee_pct=fee_pct,
                                              slippage_pct=slippage_pct,
                                              progress=_portfolio_progress)
        with _PORTFOLIO_LOCK:
            _PORTFOLIO_JOB["result"] = result
            _PORTFOLIO_JOB["error"] = result.get("error")
    except Exception as e:
        log.error("portfolio job failed: %s", type(e).__name__)
        with _PORTFOLIO_LOCK:
            _PORTFOLIO_JOB["error"] = "组合计算失败，请查看本机日志"
    finally:
        with _PORTFOLIO_LOCK:
            _PORTFOLIO_JOB["running"] = False
            _PORTFOLIO_JOB["stage"] = "done"
            _PORTFOLIO_JOB["finished_at"] = market_clock.now().isoformat(timespec="seconds")


def _portfolio_status():
    with _PORTFOLIO_LOCK:
        return copy.deepcopy(_PORTFOLIO_JOB)


def _paper_progress(stage, done, total, message):
    with _PAPER_LOCK:
        _PAPER_JOB.update({"stage": stage, "done": done, "total": total, "message": message})


def _paper_worker():
    try:
        result = paper.check(progress=_paper_progress)
        with _PAPER_LOCK:
            _PAPER_JOB["result"] = result
    except Exception as e:
        log.error("paper check failed: %s", type(e).__name__)
        with _PAPER_LOCK:
            _PAPER_JOB["error"] = "模拟检查失败，请查看本机日志"
    finally:
        with _PAPER_LOCK:
            _PAPER_JOB["running"] = False
            _PAPER_JOB["stage"] = "done"
            _PAPER_JOB["finished_at"] = market_clock.now().isoformat(timespec="seconds")


def _paper_status():
    with _PAPER_LOCK:
        return copy.deepcopy(_PAPER_JOB)


def _usage():
    if os.path.isfile(USAGE_FILE):
        try:
            with open(USAGE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_usage(u):
    try:
        with open(USAGE_FILE, "w", encoding="utf-8") as f:
            json.dump(u, f, ensure_ascii=False)
    except Exception:
        pass


def _token_key(token):
    return hashlib.sha256((token or "anon").encode("utf-8")).hexdigest()[:12]


def _loopback_bind(host):
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _valid_host_header(value):
    value = (value or "").strip().lower()
    if not value or any(c in value for c in "/\\@, \t\r\n"):
        return False
    if value in ALLOWED_HOSTS:
        return True
    try:
        parsed = urlsplit("http://" + value)
        host = parsed.hostname
        if not host or parsed.path or parsed.query or parsed.fragment:
            return False
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            return False
        if host == "localhost":
            return True
        ip = ipaddress.ip_address(host)
        return not ip.is_unspecified and (ip.is_loopback or ip.is_private or ip.is_link_local)
    except (ValueError, AttributeError):
        return False


def _safe_stock_name(name):
    return re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", str(name or "")).strip(" .")[:50].strip(" .")


def _save_history(code, name, markdown):
    """Save a report under a sanitized day/code/name path."""
    if not re.fullmatch(r"\d{6}", str(code)):
        raise ValueError("股票代码无效")
    day = market_clock.now().strftime("%Y-%m-%d")
    day_dir = os.path.join(HISTORY_ROOT, day)
    os.makedirs(day_dir, exist_ok=True)
    safe_name = _safe_stock_name(name)
    fname = f"{code}({safe_name}).md" if safe_name else f"{code}.md"
    path = _safe_history_path(f"{day}/{fname}", ".md")
    if path is None:
        raise ValueError("报告保存路径无效")
    with open(path, "w", encoding="utf-8") as f:
        title = f"{code} {safe_name}".strip()
        f.write(f"# {title} 分析（{day}）\n\n" + markdown)
    return f"{day}/{fname}"


def _list_history():
    items = []
    if not os.path.isdir(HISTORY_ROOT):
        return items
    for day in sorted(os.listdir(HISTORY_ROOT), reverse=True):
        day_dir = os.path.join(HISTORY_ROOT, day)
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day) or not os.path.isdir(day_dir):
            continue
        if os.path.normcase(os.path.dirname(os.path.realpath(day_dir))) != os.path.normcase(os.path.realpath(HISTORY_ROOT)):
            continue
        for fname in sorted(os.listdir(day_dir)):
            if not fname.endswith(".md"):
                continue
            path = _safe_history_path(f"{day}/{fname}", ".md")
            if path is None:
                continue
            m = re.match(r"^(\d{6})(?:\((.+)\))?$", fname[:-3])
            code = m.group(1) if m else fname[:-3]
            name = m.group(2) if m and m.group(2) else ""
            preview = ""
            try:
                with open(path, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if (not line or line.startswith(("#", "|"))
                                or re.fullmatch(r"[:|\- ]+", line)):
                            continue
                        preview = re.sub(r"^[-*>]+\s*", "", line)
                        preview = preview.replace("**", "").replace("`", "")
                        break
            except Exception:
                pass
            items.append({"file": f"{day}/{fname}", "code": code, "name": name, "date": day, "preview": preview[:60]})
    return items


def _safe_history_path(rel, extension):
    parts = unquote(rel).replace("\\", "/").split("/")
    if len(parts) != 2 or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", parts[0]):
        return None
    filename = parts[1]
    if extension == ".md":
        pattern = r'\d{6}(?:\([^/\\:*?"<>|\x00-\x1f]+\))?\.md'
    elif extension == ".html":
        pattern = r'\d{6}(?:\([^/\\:*?"<>|\x00-\x1f]+\))?_图表\.html'
    else:
        return None
    if not re.fullmatch(pattern, filename):
        return None
    p = os.path.realpath(os.path.join(HISTORY_ROOT, *parts))
    root = os.path.realpath(HISTORY_ROOT)
    try:
        return p if os.path.normcase(os.path.commonpath([p, root])) == os.path.normcase(root) else None
    except ValueError:
        return None


def _private_data_errors(function):
    @wraps(function)
    def guarded(self):
        try:
            return function(self)
        except private_storage.DataReadError:
            return self._send(409, json.dumps({"ok": False, "data_read_failed": True,
                "error": "账户记录无法读取；原文件保留，未自动重置，请先核对或恢复记录"}, ensure_ascii=False))
    return guarded


class Handler(BaseHTTPRequestHandler):
    def _send(self, status, body, ctype="application/json; charset=utf-8"):
        if status >= 500 and ctype.startswith("application/json"):
            body = json.dumps({"ok": False, "error": "服务处理失败，请稍后重试"}, ensure_ascii=False)
        data = body.encode("utf-8") if isinstance(body, str) else body
        route = getattr(self, "_perf_route", None)
        if route:
            runtime_metrics.record(
                route,
                getattr(self, "_perf_outcome", "error") if status < 400 else "error",
                getattr(self, "_perf_cache", "unknown"),
                (time.monotonic() - self._perf_started) * 1000,
            )
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "SAMEORIGIN")
        self.send_header("Referrer-Policy", "no-referrer")
        path = urlparse(self.path).path
        if path.startswith("/api/chart/") or path.startswith("/api/minute/"):
            self.send_header("Content-Security-Policy", "sandbox allow-scripts")
        if path.startswith("/api/"):
            self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _request_allowed(self, mutating=False):
        host = (self.headers.get("Host") or "").strip().lower()
        if not _valid_host_header(host):
            self._send(400, json.dumps({"ok": False, "error": "请求主机名不被允许"}, ensure_ascii=False))
            return False
        site = (self.headers.get("Sec-Fetch-Site") or "").lower()
        if site and site not in ("same-origin", "none") and urlparse(self.path).path.startswith("/api/"):
            self._send(403, json.dumps({"ok": False, "error": "跨站请求被拒绝"}, ensure_ascii=False))
            return False
        if mutating:
            origin = self.headers.get("Origin")
            if origin:
                parsed = urlsplit(origin)
                if (parsed.scheme not in ("http", "https") or parsed.netloc.lower() != host
                        or parsed.path or parsed.query or parsed.fragment):
                    self._send(403, json.dumps({"ok": False, "error": "请求来源不匹配"}, ensure_ascii=False))
                    return False
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                length = -1
            if length < 0 or length > MAX_REQUEST_BYTES:
                self._send(413, json.dumps({"ok": False, "error": "请求体过大或长度无效"}, ensure_ascii=False))
                return False
        return True

    def _authorized_token(self):
        if not ACCESS_TOKEN:
            return ""
        auth = self.headers.get("Authorization", "")
        token = auth[7:] if auth.startswith("Bearer ") else ""
        if not hmac.compare_digest(token, ACCESS_TOKEN):
            self._send(401, json.dumps({"ok": False, "error": "访问令牌无效"}, ensure_ascii=False))
            return None
        return token

    @_private_data_errors
    def do_GET(self):
        self._perf_started = time.monotonic()
        if not self._request_allowed():
            return
        u = urlparse(self.path)
        if u.path.startswith("/api/") and self._authorized_token() is None:
            return
        if u.path in ("/", "/index.html"):
            p = os.path.join(WEB_DIR, "index.html")
            if os.path.isfile(p):
                with open(p, "rb") as f:
                    return self._send(200, f.read(), "text/html; charset=utf-8")
        if u.path == "/api/version":
            return self._send(200, json.dumps(runtime_identity.status(), ensure_ascii=False))
        if u.path == '/api/features':
            enabled=os.getenv('SHARE_APP_MODE')!='1' and os.path.isfile(os.path.join(BASE_DIR,'announcement_lab','serve.py'))
            return self._send(200,json.dumps({'announcement_local_entry':enabled,'announcement_default_port':8010}))
        if u.path == '/api/market/time':
            return self._send(200,json.dumps(market_clock.snapshot(),ensure_ascii=False))
        if u.path == '/api/quote':
            code=parse_qs(u.query).get('code',[''])[0]
            if not re.fullmatch(r'\d{6}',code):
                return self._send(400,json.dumps({'ok':False,'error':'股票代码须为6位数字'},ensure_ascii=False))
            return self._send(200,json.dumps(holdings.public_quote(code),ensure_ascii=False,allow_nan=False))
        if u.path == "/api/research/status":
            try:
                return self._send(200,json.dumps(research_job.status(),ensure_ascii=False,allow_nan=False))
            except (OSError,ValueError,TypeError):
                return self._send(409,json.dumps({"ok":False,"error":"研究记录无法核对，原文件未重置；本次研究指标未知"},ensure_ascii=False))
        if u.path == "/api/history":
            return self._send(200, json.dumps(_list_history(), ensure_ascii=False))
        if u.path == "/api/holdings":
            return self._send(200, json.dumps(holdings.load(), ensure_ascii=False))
        if u.path == "/api/holdings/quotes":
            self._perf_route = "holdings_quotes"
            try:
                rows, self._perf_cache = holdings.quotes(with_cache_state=True)
            except private_storage.DataReadError:
                raise
            except Exception as e:
                log.error("holdings quotes failed: %s", type(e).__name__)
                return self._send(500, json.dumps({"ok": False, "error": "持仓报价暂不可用"}, ensure_ascii=False))
            self._perf_outcome = ("empty" if not rows else
                                  "unavailable" if all(row.get("price") is None for row in rows) else
                                  "partial" if any(row.get("price") is None for row in rows) else "usable")
            return self._send(200, json.dumps(rows, ensure_ascii=False))
        if u.path == "/api/diagnostics/performance":
            return self._send(200, json.dumps(runtime_metrics.snapshot(), ensure_ascii=False))
        if u.path == "/api/holdings/exit-signal":
            raw_id = parse_qs(u.query).get("id", [""])[0]
            if not raw_id.isdigit():
                return self._send(400, json.dumps({"ok": False, "error": "持仓编号无效"}, ensure_ascii=False))
            item = next((h for h in holdings.load() if h.get("id") == int(raw_id)), None)
            if item is None:
                return self._send(404, json.dumps({"ok": False, "error": "持仓记录不存在"}, ensure_ascii=False))
            try:
                result = exit_signals.exit_signal(item)
                return self._send(200, json.dumps({"ok": True, **result}, ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                log.error("holding exit signal failed for id %s: %s", raw_id, type(e).__name__)
                return self._send(500, json.dumps({"ok": False, "error": "退出条件暂不可用"}, ensure_ascii=False))
        if u.path == "/api/decision":
            self._perf_route = "decision"
            query = parse_qs(u.query)
            code = query.get("code", [""])[0]
            if not re.fullmatch(r"\d{6}", code):
                return self._send(400, json.dumps({"ok": False, "error": "股票代码须为6位数字"}, ensure_ascii=False))
            holding = None
            raw_id = query.get("holding_id", [""])[0]
            if raw_id:
                if not raw_id.isdigit():
                    return self._send(400, json.dumps({"ok": False, "error": "持仓编号无效"}, ensure_ascii=False))
                holding = next((h for h in holdings.load() if h.get("id") == int(raw_id)), None)
                if holding is None or holding.get("code") != code:
                    return self._send(404, json.dumps({"ok": False, "error": "该股票的持仓记录不存在"}, ensure_ascii=False))
            try:
                quick = query.get("quick", [""])[0] == "1" and holding is None
                result = (decision.build(code, holding, include_holdout=False)
                          if quick else decision.build(code, holding))
                self._perf_cache = result.get("request_cache_state", "unknown")
                self._perf_outcome = ("usable" if result.get("data_snapshot", {}).get("current_signal_eligible")
                                      else "unavailable")
                return self._send(200, json.dumps({"ok": True, **result}, ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                log.error("decision card failed for %s: %s", code, type(e).__name__)
                return self._send(500, json.dumps({"ok": False, "error": "决策卡暂不可用"}, ensure_ascii=False))
        if u.path == "/api/decision/replay":
            self._perf_route = "decision_replay"
            code = parse_qs(u.query).get("code", [""])[0]
            if not re.fullmatch(r"\d{6}", code):
                return self._send(400, json.dumps({"ok": False, "error": "股票代码须为6位数字"}, ensure_ascii=False))
            try:
                result = decision.replay(code)
                self._perf_cache = result.get("request_cache_state", "unknown")
                self._perf_outcome = ("usable" if result.get("holdout", {}).get("status")
                                      in ("descriptive", "no_completed_trades") else "unavailable")
                return self._send(200, json.dumps({"ok": True, **result}, ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                log.error("decision replay failed for %s: %s", code, type(e).__name__)
                return self._send(500, json.dumps({"ok": False, "error": "历史回放暂不可用"}, ensure_ascii=False))
        if u.path == "/api/rank":
            try:
                rows = stock_agent.rank_pool()
                return self._send(200, json.dumps({"ok": True, "rows": rows, "note": "价量因子排名(趋势40% 动量40% 波动20%)，不含基本面，仅供研究"}, ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                log.error("rank failed: %s", type(e).__name__)
                return self._send(500, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        if u.path == "/api/portfolio":
            try:
                result = portfolio.portfolio_analysis()
                return self._send(200, json.dumps({"ok": True, **result}, ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                log.error("portfolio failed: %s", type(e).__name__)
                return self._send(500, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        if u.path == "/api/watch":
            return self._send(200, json.dumps(watchlist.load(), ensure_ascii=False))
        if u.path == "/api/watch/quotes":
            return self._send(200, json.dumps(watchlist.quotes(), ensure_ascii=False))
        if u.path == "/api/watch/signal":
            self._perf_route = "watch_signal"
            code = parse_qs(u.query).get("code", [""])[0]
            try:
                result = entry_signals.entry_signal(code)
                self._perf_cache = result.get("request_cache_state", "unknown")
                self._perf_outcome = ("usable" if result.get("data_snapshot", {}).get("current_signal_eligible")
                                      else "unavailable")
                return self._send(200, json.dumps({"ok": True, **result}, ensure_ascii=False))
            except ValueError as e:
                return self._send(400, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                log.error("watch signal failed for %s: %s", code, type(e).__name__)
                return self._send(500, json.dumps({"ok": False, "error": "入场条件暂不可用"}, ensure_ascii=False))
        if u.path == "/api/market":
            return self._send(200, json.dumps(market.market(), ensure_ascii=False))
        if u.path == "/api/brief":
            return self._send(200, json.dumps(brief_snapshot.build(), ensure_ascii=False))
        if u.path == "/api/brief/candidates":
            try:
                return self._send(200, json.dumps(screener.cached_candidates(limit=10), ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                log.error("brief candidates failed: %s", type(e).__name__)
                return self._send(200, json.dumps({"error": "因子候选暂不可用，请稍后刷新", "rows": []}, ensure_ascii=False))
        if u.path == "/api/outlook/status":
            return self._send(200, json.dumps(outlook.status(), ensure_ascii=False))
        if u.path == "/api/portfolio/status":
            return self._send(200, json.dumps(_portfolio_status(), ensure_ascii=False))
        if u.path == "/api/paper":
            try:
                return self._send(200, json.dumps(paper.summary(), ensure_ascii=False))
            except ValueError:
                return self._send(409, json.dumps({"ok": False,
                    "error": "模拟账本无法读取或数据结构异常；原记录未重置，请先核对或恢复记录。"}, ensure_ascii=False))
        if u.path == '/api/paper/valuation':
            try:return self._send(200,json.dumps(paper.valuation(),ensure_ascii=False,allow_nan=False))
            except ValueError:
                return self._send(409,json.dumps({'ok':False,'error':'模拟持仓无法核对；当前报价估值未知，原文件未重置'},ensure_ascii=False))
        if u.path == "/api/paper/check/status":
            return self._send(200, json.dumps(_paper_status(), ensure_ascii=False))
        if u.path == "/api/settings":
            return self._send(200, json.dumps({"auto_check_enabled": bool(config.get("auto_check_enabled", False)),
                                               "auto_check_time": config.get("auto_check_time", "15:05"),
                                               "auto_scan_enabled": bool(config.get("auto_scan_enabled", False)),
                                               "auto_scan_time": config.get("auto_scan_time", "15:10"),
                                               "auto_scan_last_date": config.get("auto_scan_last_date"),
                                               "auto_scan_last_status": config.get("auto_scan_last_status"),
                                               "scan_status": screener.scan_status()}, ensure_ascii=False))
        if u.path == "/api/scan/status":
            return self._send(200, json.dumps(screener.scan_status(), ensure_ascii=False))
        if u.path.startswith("/api/minute/"):
            code = os.path.basename(u.path[len("/api/minute/"):])
            if not re.fullmatch(r"\d{6}", code):
                return self._send(400, json.dumps({"error": "invalid code"}, ensure_ascii=False))
            try:
                html = charts.build_minute_chart(code)
                if html is None:
                    return self._send(200, '<!doctype html><html lang="zh-CN"><meta charset="utf-8"><body style="font:14px \"Microsoft YaHei\",sans-serif;color:#647086;padding:16px">当前没有可用的分时数据。可以稍后重试；历史日线和持仓分析不受影响。</body></html>', "text/html; charset=utf-8")
                return self._send(200, html, "text/html; charset=utf-8")
            except private_storage.DataReadError:
                raise
            except Exception as e:
                log.info("minute chart unavailable for %s: %s", code, type(e).__name__)
                return self._send(200, '<!doctype html><html lang="zh-CN"><meta charset="utf-8"><body style="font:14px \"Microsoft YaHei\",sans-serif;color:#647086;padding:16px">分时数据源暂不可用。可以稍后重试；其他分析结果仍可查看。</body></html>', "text/html; charset=utf-8")
        if u.path.startswith("/api/history/"):
            p = _safe_history_path(u.path[len("/api/history/"):], ".md")
            if p and os.path.isfile(p):
                with open(p, encoding="utf-8") as f:
                    return self._send(200, f.read(), "text/plain; charset=utf-8")
            return self._send(404, json.dumps({"error": "not found"}, ensure_ascii=False))
        if u.path.startswith("/api/chart/"):
            p = _safe_history_path(u.path[len("/api/chart/"):], ".html")
            if p and os.path.isfile(p):
                with open(p, "rb") as f:
                    return self._send(200, f.read(), "text/html; charset=utf-8")
            return self._send(404, json.dumps({"error": "not found"}, ensure_ascii=False))
        return self._send(404, json.dumps({"error": "not found"}, ensure_ascii=False))

    @_private_data_errors
    def do_POST(self):
        self._perf_started = time.monotonic()
        if not self._request_allowed(mutating=True):
            return
        u = urlparse(self.path)
        token = ""
        if u.path.startswith("/api/"):
            token = self._authorized_token()
            if token is None:
                return
        if u.path == "/api/outlook/start":
            try:
                n = int(self.headers.get("Content-Length", 0))
                b = json.loads(self.rfile.read(n) or b"{}")
                codes = b.get("codes")
                if codes is None:
                    codes = [str(h.get("code", "")) for h in holdings.load()]
                if not isinstance(codes, list):
                    return self._send(400, json.dumps({"ok": False, "error": "codes必须为股票代码列表"}, ensure_ascii=False))
                codes = list(dict.fromkeys(str(code).strip() for code in codes))
                if not codes or len(codes) > 100 or any(not re.fullmatch(r"\d{6}", code) for code in codes):
                    return self._send(400, json.dumps({"ok": False, "error": "请输入1至100个有效的6位股票代码"}, ensure_ascii=False))
                try:
                    target_pct = float(b.get("target_pct", 10))
                    stop_pct = float(b.get("stop_pct", 5))
                except (TypeError, ValueError):
                    return self._send(400, json.dumps({"ok": False, "error": "目标涨幅和风险退出幅度必须是数字"}, ensure_ascii=False))
                if not (1 <= target_pct <= 30 and 1 <= stop_pct <= 20):
                    return self._send(400, json.dumps({"ok": False, "error": "目标涨幅需为1至30%，风险退出幅度需为1至20%"}, ensure_ascii=False))
                status = outlook.start(codes, force=bool(b.get("force", False)),
                                      target_pct=target_pct, stop_pct=stop_pct)
                return self._send(202, json.dumps({"ok": True, **status}, ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                return self._send(500, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        if u.path == "/api/portfolio/start":
            try:
                n = int(self.headers.get("Content-Length", 0))
                b = json.loads(self.rfile.read(n) or b"{}")
                pool = b.get("pool")
                if pool is not None:
                    if not isinstance(pool, list):
                        return self._send(400, json.dumps({"ok": False, "error": "股票池格式无效"}, ensure_ascii=False))
                    pool = list(dict.fromkeys(str(code).strip() for code in pool))
                    if len(pool) < 3 or len(pool) > 100 or any(not re.fullmatch(r"\d{6}", code) for code in pool):
                        return self._send(400, json.dumps({"ok": False, "error": "回测股票池需为3至100只有效股票"}, ensure_ascii=False))
                else:
                    pool = stock_agent.DEFAULT_POOL
                try:
                    fee_pct = float(b.get("fee_pct", 0.1))
                    slippage_pct = float(b.get("slippage_pct", 0.05))
                except (TypeError, ValueError):
                    return self._send(400, json.dumps({"ok": False, "error": "单边费用假设和滑点必须为数字"}, ensure_ascii=False))
                if (not math.isfinite(fee_pct) or not math.isfinite(slippage_pct)
                        or not 0 <= fee_pct <= 1 or not 0 <= slippage_pct <= 1):
                    return self._send(400, json.dumps({"ok": False, "error": "单边费用假设和滑点都需在0至1%之间"}, ensure_ascii=False))
                with _PORTFOLIO_LOCK:
                    if _PORTFOLIO_JOB["running"]:
                        return self._send(202, json.dumps({"ok": True, "already_running": True,
                                                            "job": copy.deepcopy(_PORTFOLIO_JOB)}, ensure_ascii=False))
                    _PORTFOLIO_JOB.update({"running": True, "stage": "fetch", "done": 0,
                                           "total": len(pool), "message": "准备获取历史行情",
                                           "result": None, "error": None,
                                           "started_at": market_clock.now().isoformat(timespec="seconds"),
                                           "finished_at": None, "pool": pool,
                                           "fee_pct": fee_pct, "slippage_pct": slippage_pct})
                try:
                    threading.Thread(target=_portfolio_worker,
                                     args=(pool, fee_pct, slippage_pct), daemon=True).start()
                except private_storage.DataReadError:
                    raise
                except Exception as e:
                    with _PORTFOLIO_LOCK:
                        _PORTFOLIO_JOB.update({"running": False, "stage": "error", "error": "无法启动组合任务",
                                               "finished_at": market_clock.now().isoformat(timespec="seconds")})
                    return self._send(500, json.dumps({"ok": False, "error": "无法启动后台回测"}, ensure_ascii=False))
                return self._send(202, json.dumps({"ok": True, "job": _portfolio_status()}, ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                return self._send(500, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        if u.path == "/api/research/start":
            try:
                started=research_job.start()
                return self._send(202,json.dumps({"ok":True,"already_running":not started,"job":research_job.status()},ensure_ascii=False))
            except Exception:
                return self._send(500,json.dumps({"ok":False,"error":"无法启动冻结历史研究"},ensure_ascii=False))
        if u.path == "/api/scan/start":
            self._perf_route = "scan_start"
            try:
                started = screener.start_scan(refresh=False)
                status = screener.scan_status()
            except private_storage.DataReadError:
                raise
            except Exception as e:
                log.error("scan start failed: %s", type(e).__name__)
                return self._send(500, json.dumps({"ok": False, "error": "扫描暂无法启动"}, ensure_ascii=False))
            self._perf_outcome = "accepted" if started else "busy" if status.get("running") else "error"
            return self._send(200, json.dumps({"ok": started,
                                               "already_running": bool(not started and status.get("running")),
                                               "scan": status}, ensure_ascii=False))
        if u.path == "/api/paper/check":
            with _PAPER_LOCK:
                if _PAPER_JOB["running"]:
                    return self._send(202, json.dumps({"ok": True, "already_running": True,
                                                       "job": copy.deepcopy(_PAPER_JOB)}, ensure_ascii=False))
                _PAPER_JOB.update({"running": True, "stage": "starting", "done": 0,
                                   "total": len(screener.UNIVERSE), "message": "准备执行模拟策略",
                                   "result": None, "error": None,
                                   "started_at": market_clock.now().isoformat(timespec="seconds"),
                                   "finished_at": None})
            try:
                threading.Thread(target=_paper_worker, daemon=True).start()
                return self._send(202, json.dumps({"ok": True, "job": _paper_status()}, ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                with _PAPER_LOCK:
                    _PAPER_JOB.update({"running": False, "stage": "error", "error": "无法启动模拟检查",
                                       "finished_at": market_clock.now().isoformat(timespec="seconds")})
                return self._send(500, json.dumps({"ok": False, "error": "无法启动模拟检查"}, ensure_ascii=False))
        if u.path == "/api/paper/reset":
            try:
                paper.reset()
                return self._send(200, json.dumps({"ok": True}, ensure_ascii=False))
            except RuntimeError as e:
                return self._send(409, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        if u.path == "/api/paper/rules":
            try:
                n = int(self.headers.get("Content-Length", 0))
                b = json.loads(self.rfile.read(n) or b"{}")
                values = {}
                if b.get("buy_top_n") is not None:
                    value = int(b["buy_top_n"])
                    if not 0 <= value <= 10:
                        return self._send(400, json.dumps({"ok": False, "error": "买入候选数需为0至10"}, ensure_ascii=False))
                    values["buy_top_n"] = value
                if b.get("buy_min_score") is not None:
                    value = int(b["buy_min_score"])
                    if not 0 <= value <= 100:
                        return self._send(400, json.dumps({"ok": False, "error": "最低因子分需为0至100"}, ensure_ascii=False))
                    values["buy_min_score"] = value
                if b.get("max_position_pct") is not None:
                    value = int(b["max_position_pct"])
                    if not 1 <= value <= 100:
                        return self._send(400, json.dumps({"ok": False, "error": "单只仓位上限需为1至100%"}, ensure_ascii=False))
                    values["max_position_pct"] = value
                if b.get("max_trades_per_day") is not None:
                    value = int(b["max_trades_per_day"])
                    if not 1 <= value <= 20:
                        return self._send(400, json.dumps({"ok": False, "error": "单日交易上限需为1至20次"}, ensure_ascii=False))
                    values["max_trades_per_day"] = value
                for key, label, low, high in (("broker_fee_pct", "券商佣金率", 0, 0.3),
                                               ("slippage_pct", "单边滑点", 0, 2),
                                               ("min_commission", "单笔最低佣金", 0, 100)):
                    if b.get(key) is not None:
                        value = float(b[key])
                        if not math.isfinite(value) or not low <= value <= high:
                            return self._send(400, json.dumps({"ok": False, "error": f"{label}超出范围"}, ensure_ascii=False))
                        values[key] = value
                r = paper.update_rules(values)
                return self._send(200, json.dumps({"ok": True, "rules": r}, ensure_ascii=False))
            except paper.LedgerReadError as e:
                return self._send(409, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
            except RuntimeError as e:
                return self._send(409, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                return self._send(500, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        if u.path == "/api/screen":
            try:
                n = int(self.headers.get("Content-Length", 0))
                b = json.loads(self.rfile.read(n) or b"{}")
                rules = b.get("rules") or {}
                universe = b.get("universe") or "sample"
                coverage = screener.cache_coverage(universe)
                if universe == "full" and coverage.get("universe_error"):
                    return self._send(200, json.dumps({"ok": False,
                                                       "error": coverage["universe_error"],
                                                       "coverage": coverage}, ensure_ascii=False))
                if rules.get("pe_max") is not None and coverage["pe_cached_count"] == 0:
                    return self._send(200, json.dumps({"ok": False,
                                                       "error": "当前因子库没有估值分位缓存。请移除估值条件，或在个股诊断中查看估值；全市场逐只拉取基本面会很慢。",
                                                       "coverage": coverage}, ensure_ascii=False))
                rows = screener.screen(rules, universe=universe, refresh_missing=False)
                return self._send(200, json.dumps({"ok": True, "rows": rows,
                                                  "universe": coverage["universe_count"],
                                                  "coverage": coverage,
                                                  "universe_type": universe,
                                                  "score_note": "综合分是趋势40%、动量40%、波动20%的价量排序分，不是上涨概率；默认排除ST、*ST和退市类股票。"}, ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                return self._send(500, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        if u.path == "/api/portfolio":
            try:
                n = int(self.headers.get("Content-Length", 0))
                b = json.loads(self.rfile.read(n) or b"{}")
                pool = b.get("pool") or None
                fee_pct = float(b.get("fee_pct", 0.1))
                slippage_pct = float(b.get("slippage_pct", 0.05))
                if (not math.isfinite(fee_pct) or not math.isfinite(slippage_pct)
                        or not 0 <= fee_pct <= 1 or not 0 <= slippage_pct <= 1):
                    return self._send(400, json.dumps({"ok": False, "error": "单边费用假设和滑点都需在0至1%之间"}, ensure_ascii=False))
                result = portfolio.portfolio_analysis(pool=pool, fee_pct=fee_pct,
                                                      slippage_pct=slippage_pct)
                return self._send(200, json.dumps({"ok": True, **result}, ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                log.error("portfolio failed: %s", type(e).__name__)
                return self._send(500, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        if u.path == "/api/watch":
            try:
                n = int(self.headers.get("Content-Length", 0))
                b = json.loads(self.rfile.read(n) or b"{}")
                code = str(b.get("code", "")).strip()
                if not re.fullmatch(r"\d{6}", code):
                    return self._send(400, json.dumps({"ok": False, "error": "请输入6位数字股票代码"}, ensure_ascii=False))
                items = watchlist.add(code)
                return self._send(200, json.dumps({"ok": True, "items": items}, ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                return self._send(500, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        if u.path == "/api/settings/key":
            try:
                n = int(self.headers.get("Content-Length", 0))
                b = json.loads(self.rfile.read(n) or b"{}")
                key = str(b.get("key", "")).strip()
                if not key:
                    return self._send(400, json.dumps({"ok": False, "error": "Key不能为空"}, ensure_ascii=False))
                config.set_key(key)
                return self._send(200, json.dumps({"ok": True}, ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                return self._send(500, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        if u.path == "/api/settings/auto":
            try:
                n = int(self.headers.get("Content-Length", 0))
                b = json.loads(self.rfile.read(n) or b"{}")
                updates = {}
                for key in ("auto_check_enabled", "auto_scan_enabled"):
                    if key in b:
                        updates[key] = bool(b[key])
                for key in ("auto_check_time", "auto_scan_time"):
                    if key in b:
                        value = str(b[key]).strip()
                        if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", value):
                            return self._send(400, json.dumps({"ok": False, "error": "时间格式应为 HH:MM"}, ensure_ascii=False))
                        updates[key] = value
                if updates:
                    config.update(updates)
                return self._send(200, json.dumps({"ok": True}, ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                return self._send(500, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        if u.path == "/api/holdings":
            try:
                n = int(self.headers.get("Content-Length", 0))
                b = json.loads(self.rfile.read(n) or b"{}")
                code = str(b.get("code", "")).strip()
                if not re.fullmatch(r"\d{6}", code):
                    return self._send(400, json.dumps({"ok": False, "error": "请输入6位数字股票代码"}, ensure_ascii=False))
                try:
                    cost = float(b.get("cost_price", 0))
                    shares = float(b.get("shares", 0))
                except (TypeError, ValueError):
                    return self._send(400, json.dumps({"ok": False, "error": "成本价和股数必须是数字"}, ensure_ascii=False))
                if not math.isfinite(cost) or not math.isfinite(shares) or cost <= 0 or shares <= 0:
                    return self._send(400, json.dumps({"ok": False, "error": "成本价和股数必须大于0"}, ensure_ascii=False))
                acquired_on = str(b.get("acquired_on") or "").strip()
                if acquired_on:
                    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", acquired_on):
                        return self._send(400, json.dumps({"ok": False, "error": "买入日期须为 YYYY-MM-DD"}, ensure_ascii=False))
                    try:
                        parsed_date = date.fromisoformat(acquired_on)
                    except ValueError:
                        return self._send(400, json.dumps({"ok": False, "error": "买入日期须为 YYYY-MM-DD"}, ensure_ascii=False))
                    if parsed_date > datetime.now(entry_signals.SHANGHAI).date():
                        return self._send(400, json.dumps({"ok": False, "error": "买入日期不能在未来"}, ensure_ascii=False))
                item = holdings.add(code, cost, shares, acquired_on)
                return self._send(200, json.dumps({"ok": True, "item": item}, ensure_ascii=False))
            except private_storage.DataReadError:
                raise
            except Exception as e:
                return self._send(500, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        if u.path != "/api/analyze":
            return self._send(404, json.dumps({"error": "not found"}, ensure_ascii=False))
        tk = _token_key(token)
        today = market_clock.now().strftime("%Y-%m-%d")
        usage = _usage()
        if usage.get(today, {}).get(tk, 0) >= DAILY_LIMIT:
            return self._send(429, json.dumps({"ok": False, "error": f"今日额度已用尽（{DAILY_LIMIT}次/日）"}, ensure_ascii=False))
        try:
            n = int(self.headers.get("Content-Length", 0))
            code = str(json.loads(self.rfile.read(n) or b"{}").get("code", "")).strip()
            if not re.fullmatch(r"\d{6}", code):
                return self._send(400, json.dumps({"ok": False, "error": "请输入6位数字股票代码"}, ensure_ascii=False))
            if not stock_agent.current_key():
                return self._send(409, json.dumps({"ok": False, "error": "未配置 DeepSeek Key，请到设置页保存"}, ensure_ascii=False))
            if not _sem.acquire(blocking=False):
                return self._send(503, json.dumps({"ok": False, "error": "服务器繁忙，请稍后再试"}, ensure_ascii=False))
            try:
                log.info(f"analyze {code} start")
                markdown = stock_agent.analyze(code)
                name = stock_agent.get_stock_name(code)
                rel = _save_history(code, name, markdown)
                chart_rel = None
                try:
                    cdf = stock_agent.get_chart_data(code)
                    if cdf is not None:
                        html = charts.build_chart_html(cdf)
                        day = rel.split("/")[0]
                        day_dir = os.path.join(HISTORY_ROOT, day)
                        os.makedirs(day_dir, exist_ok=True)
                        safe_name = _safe_stock_name(name)
                        cname = f"{code}({safe_name})_图表.html" if safe_name else f"{code}_图表.html"
                        chart_path = _safe_history_path(f"{day}/{cname}", ".html")
                        if chart_path is None:
                            raise ValueError("图表保存路径无效")
                        with open(chart_path, "w", encoding="utf-8") as cf:
                            cf.write(html)
                        chart_rel = f"{day}/{cname}"
                except private_storage.DataReadError:
                    raise
                except Exception as e:
                    log.error("chart failed: %s", type(e).__name__)
                usage.setdefault(today, {})[tk] = usage.get(today, {}).get(tk, 0) + 1
                _save_usage(usage)
                log.info(f"analyze {code} ok -> {rel}")
                return self._send(200, json.dumps({"ok": True, "markdown": markdown, "file": rel, "chart": chart_rel}, ensure_ascii=False))
            finally:
                _sem.release()
        except Exception as e:
            log.error("analyze failed: %s", type(e).__name__)
            return self._send(500, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))

    @_private_data_errors
    def do_PUT(self):
        if not self._request_allowed(mutating=True):
            return
        u = urlparse(self.path)
        if u.path.startswith("/api/") and self._authorized_token() is None:
            return
        m = re.match(r"^/api/holdings/(\d+)$", u.path)
        if not m:
            return self._send(404, json.dumps({"error": "not found"}, ensure_ascii=False))
        try:
            n = int(self.headers.get("Content-Length", 0))
            b = json.loads(self.rfile.read(n) or b"{}")
            cost = b.get("cost_price")
            shares = b.get("shares")
            try:
                cost = float(cost) if cost is not None else None
                shares = float(shares) if shares is not None else None
            except (TypeError, ValueError):
                return self._send(400, json.dumps({"ok": False, "error": "成本价和股数必须是数字"}, ensure_ascii=False))
            if cost is not None and (not math.isfinite(cost) or cost <= 0):
                return self._send(400, json.dumps({"ok": False, "error": "成本价必须大于0"}, ensure_ascii=False))
            if shares is not None and (not math.isfinite(shares) or shares <= 0):
                return self._send(400, json.dumps({"ok": False, "error": "股数必须大于0"}, ensure_ascii=False))
            acquired_on = b.get("acquired_on")
            if acquired_on is not None:
                acquired_on = str(acquired_on).strip()
                if acquired_on:
                    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", acquired_on):
                        return self._send(400, json.dumps({"ok": False, "error": "买入日期须为 YYYY-MM-DD"}, ensure_ascii=False))
                    try:
                        parsed_date = date.fromisoformat(acquired_on)
                    except ValueError:
                        return self._send(400, json.dumps({"ok": False, "error": "买入日期须为 YYYY-MM-DD"}, ensure_ascii=False))
                    if parsed_date > datetime.now(entry_signals.SHANGHAI).date():
                        return self._send(400, json.dumps({"ok": False, "error": "买入日期不能在未来"}, ensure_ascii=False))
            item = holdings.update(int(m.group(1)), cost_price=cost, shares=shares,
                                   acquired_on=acquired_on)
            if item is None:
                return self._send(404, json.dumps({"ok": False, "error": "记录不存在"}, ensure_ascii=False))
            return self._send(200, json.dumps({"ok": True, "item": item}, ensure_ascii=False))
        except Exception as e:
            return self._send(500, json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))

    @_private_data_errors
    def do_DELETE(self):
        if not self._request_allowed(mutating=True):
            return
        u = urlparse(self.path)
        if u.path.startswith("/api/") and self._authorized_token() is None:
            return
        m2 = re.match(r"^/api/watch/(\d{6})$", u.path)
        if m2:
            items = watchlist.remove(m2.group(1))
            return self._send(200, json.dumps({"ok": True, "items": items}, ensure_ascii=False))
        m = re.match(r"^/api/holdings/(\d+)$", u.path)
        if not m:
            return self._send(404, json.dumps({"error": "not found"}, ensure_ascii=False))
        ok = holdings.delete(int(m.group(1)))
        return self._send(200, json.dumps({"ok": ok}, ensure_ascii=False))

    def log_message(self, *args):
        pass


def _lan_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return None


def _scheduler_tick(at, last_run):
    now = market_clock.aware(at)
    if market_clock.day_status(now.date())["is_trading_day"] is not True:
        return last_run
    today = now.date().isoformat()
    if config.get("auto_check_enabled", False):
        hhmm = str(config.get("auto_check_time", "15:05"))
        target = datetime.combine(now.date(), datetime.strptime(hhmm, "%H:%M").time(), now.tzinfo)
        if 0 <= (now - target).total_seconds() < 120 and last_run != today:
            paper.check()
            log.info("模拟交易自动检查完成")
            last_run = today
    if config.get("auto_scan_enabled", False):
        hhmm = str(config.get("auto_scan_time", "15:10"))
        target = datetime.combine(now.date(), datetime.strptime(hhmm, "%H:%M").time(), now.tzinfo)
        if now >= target and config.get("auto_scan_last_date") != today:
            if screener.scan_status()["running"]:
                config.update({"auto_scan_last_date": today, "auto_scan_last_status": "already_running"})
                log.info("全市场因子库定时更新与现有扫描合并")
            elif screener.start_scan(refresh=True):
                config.update({"auto_scan_last_date": today, "auto_scan_last_status": "started"})
                log.info("全市场因子库定时更新已启动")
    return last_run


def _scheduler():
    import time as _time
    last_run = None
    while True:
        try:
            last_run = _scheduler_tick(market_clock.now(), last_run)
        except Exception as e:
            log.error("scheduler error: %s", type(e).__name__)
        _time.sleep(30)


def main():
    if (not _loopback_bind(HOST) or ALLOWED_HOSTS) and not ACCESS_TOKEN:
        print("启动被拒绝：局域网/自定义域名访问必须先设置 ACCESS_TOKEN。")
        print("仅本机使用请保持 WEB_BIND_HOST=127.0.0.1；共享请先设置令牌再显式配置 WEB_BIND_HOST。")
        return
    local_host = "127.0.0.1" if HOST == "0.0.0.0" else HOST
    try:
        server = ThreadingHTTPServer((HOST, PORT), Handler)
    except OSError as e:
        print(f"启动失败：端口 {PORT} 可能被占用（{e}）")
        return
    print("A股智能分析网页已启动：")
    print(f"  本机访问：http://{local_host}:{PORT}")
    print(f"  鉴权：{'已开启（需访问令牌）' if ACCESS_TOKEN else '未开启'}; 额度：{DAILY_LIMIT}次/日")
    if not _loopback_bind(HOST):
        ip = _lan_ip() if HOST == "0.0.0.0" else HOST
        if ip:
            print(f"  局域网访问（同一网络）：http://{ip}:{PORT}")
        print("  安全提示：普通 HTTP 不加密；跨网络分享请在受控 HTTPS 代理或隧道后使用，并保管好令牌。")
        if len(ACCESS_TOKEN) < 16:
            print("  安全提示：访问令牌较短，建议使用至少16个随机字符。")
    print("按 Ctrl+C 停止")
    if os.getenv("WEB_NO_BROWSER") != "1":
        try:
            import webbrowser
            webbrowser.open(f"http://{local_host}:{PORT}")
        except Exception:
            pass
    threading.Thread(target=_scheduler, daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")


if __name__ == "__main__":
    main()
