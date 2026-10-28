# -*- coding: utf-8 -*-
"""Account gateway maintained with the current allowlisted business app.

Each account runs a private loopback copy of web_app.py. The gateway owns login,
sessions and routing; browser clients never receive the backend owner token.
"""
from __future__ import annotations

import hashlib
import gzip
import hmac
import html
import http.client
import json
import os
import re
import secrets
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.request
from dataclasses import dataclass
from contextlib import contextmanager
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from app_manifest import APP_FILES, APP_VERSION
from runtime_identity import disk_identity


SOURCE = Path(__file__).resolve().parent
DATA = Path(os.getenv("SHARE_DATA_ROOT", str(SOURCE / "share_data"))).resolve()
DB = DATA / "accounts.sqlite3"
BIND = os.getenv("SHARE_BIND_HOST", "127.0.0.1")
PORT = int(os.getenv("SHARE_PORT", "8080"))
MAX_ACCOUNTS = int(os.getenv("SHARE_MAX_ACCOUNTS", "5"))
COOKIE = "ashare_session"
SESSION_SECONDS = 7 * 24 * 60 * 60
MAX_BODY = 1024 * 1024
AUTH_BODY = 4096
_BACKENDS: dict[str, "Backend"] = {}
_BACKEND_LOCK = threading.Lock()
_START_LOCKS = {}
_STOPPING = False
_FAILED: dict[str, list[float]] = {}
_FAILED_LOCK = threading.Lock()


@dataclass
class Backend:
    port: int
    token: str
    process: subprocess.Popen
    log_file: object


@contextmanager
def connect():
    db = sqlite3.connect(DB, timeout=15)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    try:
        with db:
            yield db
    finally:
        db.close()


def init_db():
    DATA.mkdir(parents=True, exist_ok=True)
    try:
        DATA.chmod(0o700)
    except OSError:
        pass
    with connect() as db:
        db.execute("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, username TEXT UNIQUE NOT NULL, salt BLOB NOT NULL, password_hash BLOB NOT NULL, created_at INTEGER NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS invites (code_hash TEXT PRIMARY KEY, label TEXT NOT NULL, created_at INTEGER NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS sessions (token_hash TEXT PRIMARY KEY, user_id TEXT NOT NULL REFERENCES users(id), expires_at INTEGER NOT NULL)")


def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def password_hash(password, salt):
    return hashlib.scrypt(password.encode("utf-8"), salt=salt, n=2**14, r=8, p=1, dklen=32)


def create_invite(label):
    with connect() as db:
        count = db.execute("SELECT count(*) FROM users").fetchone()[0]
        pending = db.execute("SELECT count(*) FROM invites").fetchone()[0]
        if count + pending >= MAX_ACCOUNTS:
            raise RuntimeError("账户与待用邀请码已达到上限")
        code = secrets.token_urlsafe(24)
        db.execute("INSERT INTO invites VALUES (?,?,?)", (digest(code), label[:80], int(time.time())))
    return code


def register(invite, username, password):
    if not re.fullmatch(r"[A-Za-z0-9_-]{3,24}", username):
        return None, "用户名需为 3–24 位字母、数字、下划线或连字符"
    if len(password) < 12 or len(password) > 128:
        return None, "密码需为 12–128 个字符"
    user_id = secrets.token_hex(16)
    now = int(time.time())
    with connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT 1 FROM invites WHERE code_hash=?", (digest(invite),)).fetchone()
        if row is None:
            return None, "邀请码无效或已经使用"
        if db.execute("SELECT count(*) FROM users").fetchone()[0] >= MAX_ACCOUNTS:
            return None, "账户数量已达上限"
        if db.execute("SELECT 1 FROM users WHERE username=?", (username,)).fetchone():
            return None, "这个用户名已被使用"
        salt = secrets.token_bytes(16)
        db.execute("INSERT INTO users VALUES (?,?,?,?,?)", (user_id, username, salt, password_hash(password, salt), now))
        db.execute("DELETE FROM invites WHERE code_hash=?", (digest(invite),))
    return user_id, None


def login(username, password):
    with connect() as db:
        row = db.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    salt = row["salt"] if row else bytes(16)
    candidate = password_hash(password, salt)
    if row and hmac.compare_digest(candidate, row["password_hash"]):
        return row["id"]
    return None


def new_session(user_id):
    token = secrets.token_urlsafe(32)
    expiry = int(time.time()) + SESSION_SECONDS
    with connect() as db:
        db.execute("DELETE FROM sessions WHERE expires_at<?", (int(time.time()),))
        db.execute("INSERT INTO sessions VALUES (?,?,?)", (digest(token), user_id, expiry))
    return token


def user_for_session(token):
    if not token:
        return None
    with connect() as db:
        row = db.execute("SELECT users.id, users.username FROM sessions JOIN users ON users.id=sessions.user_id WHERE sessions.token_hash=? AND sessions.expires_at>?", (digest(token), int(time.time()))).fetchone()
    return dict(row) if row else None


def clear_session(token):
    if token:
        with connect() as db:
            db.execute("DELETE FROM sessions WHERE token_hash=?", (digest(token),))


def allowed_login_attempt(key, limit=10):
    now = time.monotonic()
    with _FAILED_LOCK:
        if len(_FAILED) > 1000:
            for old_key in list(_FAILED):
                recent = [t for t in _FAILED[old_key] if now - t < 900]
                if recent:
                    _FAILED[old_key] = recent
                else:
                    del _FAILED[old_key]
        attempts = [t for t in _FAILED.get(key, []) if now - t < 900]
        _FAILED[key] = attempts
        return len(attempts) < limit


def record_failed_login(key):
    with _FAILED_LOCK:
        _FAILED.setdefault(key, []).append(time.monotonic())


def _free_loopback_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _prepare_user_app(user_id):
    if not re.fullmatch(r"[0-9a-f]{32}", user_id):
        raise ValueError("账户标识无效")
    if any(not (SOURCE / relative).is_file() for relative in APP_FILES):
        raise RuntimeError("发行包缺少必要业务模块")
    app = DATA / "users" / user_id / "app"
    app.mkdir(parents=True, exist_ok=True)
    try:
        app.chmod(0o700)
    except OSError:
        pass
    (app / "web").mkdir(exist_ok=True)
    for relative in APP_FILES:
        source_file = SOURCE / relative
        if not source_file.is_file():
            raise RuntimeError("发行包缺少必要业务模块")
        shutil.copy2(source_file, app / relative)
    return app


def ensure_backend(user_id):
    with _BACKEND_LOCK:
        if _STOPPING:
            raise RuntimeError("网关正在关闭")
        start_lock = _START_LOCKS.setdefault(user_id, threading.Lock())
    with start_lock:
        with _BACKEND_LOCK:
            current = _BACKENDS.get(user_id)
        if current and current.process.poll() is None:
            return current
        if current:
            try:
                current.log_file.close()
            except OSError:
                pass
            with _BACKEND_LOCK:
                del _BACKENDS[user_id]
        app = _prepare_user_app(user_id)
        expected = disk_identity(app)
        port = _free_loopback_port()
        token = secrets.token_urlsafe(32)
        env = os.environ.copy()
        for name in ("DEEPSEEK_API_KEY", "ACCESS_TOKEN", "WEB_ALLOWED_HOSTS", "SHARE_DATA_ROOT", "PUBLIC_DAILY_CACHE_DB"):
            env.pop(name, None)
        for name in list(env):
            if name.startswith('ANNOUNCEMENT_'):
                env.pop(name)
        env.update({"WEB_BIND_HOST": "127.0.0.1", "WEB_PORT": str(port), "ACCESS_TOKEN": token,
                    "WEB_NO_BROWSER": "1", "PYTHONUNBUFFERED": "1", "SHARE_APP_MODE": "1"})
        if os.getenv("SHARE_PUBLIC_CACHE", "1") != "0":
            env["PUBLIC_DAILY_CACHE_DB"] = str(DATA / "public_market" / "daily.sqlite3")
        log_file = open(app / "runtime.log", "ab", buffering=0)
        process = subprocess.Popen([sys.executable, "web_app.py"], cwd=app, env=env,
                                   stdin=subprocess.DEVNULL, stdout=log_file, stderr=subprocess.STDOUT)
        backend = Backend(port, token, process, log_file)
        deadline = time.monotonic() + 35
        while time.monotonic() < deadline:
            if process.poll() is not None:
                break
            try:
                request = urllib.request.Request(f"http://127.0.0.1:{port}/api/version",
                                                 headers={"Authorization": "Bearer " + token})
                with urllib.request.urlopen(request, timeout=1) as response:
                    loaded = json.load(response)
                    if (response.status == 200 and loaded.get("app_version") == APP_VERSION
                            and loaded.get("code_sha256") == expected["code_sha256"]
                            and not loaded.get("missing_files") and loaded.get("disk_matches_startup")):
                        with _BACKEND_LOCK:
                            if _STOPPING:
                                break
                            _BACKENDS[user_id] = backend
                        return backend
            except Exception:
                time.sleep(0.25)
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        log_file.close()
        raise RuntimeError("账户服务启动失败；请检查该账户 runtime.log")


def stop_backends():
    global _STOPPING
    with _BACKEND_LOCK:
        _STOPPING = True
        for backend in _BACKENDS.values():
            if backend.process.poll() is None:
                backend.process.terminate()
                try:
                    backend.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    backend.process.kill()
                    backend.process.wait(timeout=5)
            backend.log_file.close()
        _BACKENDS.clear()


def restore_backends():
    # Keep each account's original scheduler active after a gateway restart.
    with connect() as db:
        ids = [row[0] for row in db.execute("SELECT id FROM users")]
    for user_id in ids:
        try:
            ensure_backend(user_id)
        except Exception:
            # A broken account process must not prevent the login gateway starting.
            pass


def login_html(mode, error=""):
    registering = mode == "register"
    title = "创建账户" if registering else "登录账户"
    action = "/auth/register" if registering else "/auth/login"
    invite_field = '<label>一次性邀请码<input name="invite" autocomplete="off" required maxlength="128"></label>' if registering else ""
    username_help = ('<small class="hint">3–24 位，只能使用英文字母、数字、下划线或连字符。</small>'
                     if registering else '<small class="hint">请按注册时的用户名原样输入，区分大小写；邀请码不是登录密码。</small>')
    password_help = '<small class="hint">密码需 12–128 个字符，请自行设置并妥善保存。</small>' if registering else ""
    privacy_field = '<label class="consent"><input name="privacy_ack" type="checkbox" value="1" required>我已了解：分享链接经第三方 cpolar 转发；本机机主可以接触我在这里保存的持仓、自选、模拟和分析记录。</label>' if registering else ""
    alternate = '<a href="/login">已有账户？去登录</a>' if registering else '<a href="/register">收到邀请码？创建账户</a>'
    error_box = f'<p class="error" role="alert">{html.escape(error)}</p>' if error else ""
    no_validate = " novalidate" if registering else ""
    feedback_box = '<p id="form-feedback" class="error" role="alert" hidden></p>' if registering else ""
    validation_script = """<script>
    const form = document.querySelector('form');
    const feedback = document.getElementById('form-feedback');
    form.addEventListener('submit', function (event) {
      const invite = form.elements.invite.value.trim();
      const username = form.elements.username.value.trim();
      const passwordLength = Array.from(form.elements.password.value).length;
      let message = '';
      let field = null;
      if (!invite) { message = '请输入一次性邀请码。'; field = form.elements.invite; }
      else if (!/^[A-Za-z0-9_-]{3,24}$/.test(username)) { message = '用户名需为 3–24 位英文字母、数字、下划线或连字符。'; field = form.elements.username; }
      else if (passwordLength < 12 || passwordLength > 128) { message = '密码需为 12–128 个字符。'; field = form.elements.password; }
      else if (!form.elements.privacy_ack.checked) { message = '请先勾选数据流转说明。'; field = form.elements.privacy_ack; }
      if (message) {
        event.preventDefault();
        feedback.textContent = message;
        feedback.hidden = false;
        field.focus();
        return;
      }
      feedback.textContent = '正在创建账户，请稍候…';
      feedback.hidden = false;
      form.querySelector('button').disabled = true;
    });
    </script>""" if registering else ""
    return f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow"><title>{title} · A股智能助手 {APP_VERSION}</title><style>
    *{{box-sizing:border-box}}body{{margin:0;min-height:100vh;display:grid;place-items:center;background:#f4f6fa;color:#192335;font-family:"Segoe UI","Microsoft YaHei",sans-serif;padding:18px}}main{{width:min(430px,100%);background:#fff;border:1px solid #e1e6ef;border-radius:18px;box-shadow:0 14px 50px #172b4b12;padding:29px}}.brand{{color:#2864dc;font-weight:800;font-size:13px}}h1{{font-size:27px;margin:9px 0 4px}}p{{font-size:12px;color:#647086;line-height:1.7;margin:0 0 19px}}label{{display:block;font-size:12px;color:#495971;margin:14px 0}}input{{display:block;width:100%;margin-top:6px;border:1px solid #cfd8e6;border-radius:9px;padding:11px;font:inherit;font-size:15px}}.hint{{display:block;margin-top:6px;color:#647086;font-size:12px;line-height:1.5}}.consent{{line-height:1.6}}.consent input{{display:inline-block;width:auto;margin:0 7px 0 0;vertical-align:middle}}button{{width:100%;border:0;border-radius:9px;background:#2864dc;color:#fff;font-weight:700;padding:12px;margin-top:9px;font:inherit;cursor:pointer}}a{{display:block;text-align:center;margin-top:16px;color:#2864dc;font-size:12px;text-decoration:none}}.error{{color:#ac2b35;background:#fff2f3;border-radius:8px;padding:9px}}.fine{{border-top:1px solid #e1e6ef;padding-top:14px;margin:21px 0 0}}
    </style></head><body><main><div class="brand">A股智能助手 · {APP_VERSION}</div><h1>{title}</h1><p>同一个账户在电脑和手机打开相同的功能与数据。行情来自实时或最近可用的公开数据源，请核对页面日期与质量提示。</p>{error_box}{feedback_box}<form method="post" action="{action}"{no_validate}>{invite_field}<label>用户名<input name="username" autocomplete="username" required maxlength="24">{username_help}</label><label>密码<input name="password" type="password" autocomplete="{'new-password' if registering else 'current-password'}" required maxlength="128">{password_help}</label>{privacy_field}<button type="submit">{title}</button></form>{alternate}<p class="fine">仅供学习研究；策略尚未验证，不保证收益。</p></main>{validation_script}</body></html>'''


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format_string, *args):
        # Do not write URL parameters or cookies to logs.
        pass

    def _send(self, status, body, content_type="text/html; charset=utf-8", headers=None):
        data = body.encode("utf-8") if isinstance(body, str) else body
        extra = dict(headers or {})
        if (len(data) >= 2048 and "gzip" in self.headers.get("Accept-Encoding", "").lower()
                and content_type.startswith(("text/", "application/json"))
                and "Content-Encoding" not in extra):
            compressed = gzip.compress(data, compresslevel=5)
            if len(compressed) < len(data):
                data = compressed
                extra["Content-Encoding"] = "gzip"
                extra["Vary"] = "Accept-Encoding"
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        for key, value in extra.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(data)

    def _redirect(self, target, cookie=None):
        headers = {"Location": target}
        if cookie is not None:
            host = self.headers.get("Host", "").lower().split(":", 1)[0]
            secure = "" if host in ("127.0.0.1", "localhost", "[::1]") else "; Secure"
            value = f"{COOKIE}={cookie}; HttpOnly; SameSite=Lax; Path=/; Max-Age={SESSION_SECONDS}{secure}"
            if not cookie:
                value = f"{COOKIE}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0{secure}"
            headers["Set-Cookie"] = value
        self._send(303, b"", headers=headers)

    def _session_token(self):
        try:
            cookie = SimpleCookie(self.headers.get("Cookie", ""))
            value = cookie.get(COOKIE)
            return value.value if value else ""
        except Exception:
            return ""

    def _require_user(self):
        user = user_for_session(self._session_token())
        if user is None:
            if self.path.startswith("/api/"):
                self._send(401, json.dumps({"ok": False, "error": "登录已失效"}, ensure_ascii=False), "application/json; charset=utf-8")
            else:
                self._redirect("/login")
        return user

    def _same_origin(self):
        site = self.headers.get("Sec-Fetch-Site", "").lower()
        origin = self.headers.get("Origin", "")
        host = self.headers.get("Host", "")
        reason = ""
        if site and site not in ("same-origin", "none"):
            reason = "fetch-site"
        elif site == "same-origin" and origin in ("", "null"):
            # Some browsers omit Origin or send an opaque Origin even for a
            # request that they mark as same-origin. Fetch Metadata is the
            # browser-controlled primary CSRF signal in this case.
            pass
        elif origin:
            parsed = urlsplit(origin)
            if parsed.netloc.lower() != host.lower() or parsed.scheme not in ("http", "https"):
                reason = "origin-host"
        else:
            reason = "missing-origin"
        return not reason

    def _reject_cross_site(self):
        # Reject before reading the body, so the connection must not be reused.
        self.close_connection = True
        self._send(403, "跨站请求被拒绝", headers={"Connection": "close"})

    def _body(self, limit):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = -1
        if length < 0 or length > limit:
            self._send(413, "请求体过大")
            return None
        return self.rfile.read(length)

    def _proxy(self, backend, body=None):
        port = backend.port
        headers = {"Host": f"127.0.0.1:{port}", "Authorization": f"Bearer {backend.token}", "Connection": "close"}
        for key in ("Accept", "Content-Type"):
            if self.headers.get(key):
                headers[key] = self.headers[key]
        if self.headers.get("Origin"):
            headers["Origin"] = f"http://127.0.0.1:{port}"
        if body is not None:
            headers["Content-Length"] = str(len(body))
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=300)
        try:
            connection.request(self.command, self.path, body=body, headers=headers)
            response = connection.getresponse()
            content = response.read()
            out = {}
            for key in ("Content-Type", "Cache-Control", "Content-Security-Policy"):
                if response.getheader(key):
                    out[key] = response.getheader(key)
            self._send(response.status, content, out.pop("Content-Type", "application/octet-stream"), out)
        finally:
            connection.close()

    def do_GET(self):
        if not self.path.startswith("/") or self.path.startswith("//") or "\\" in self.path:
            return self._send(400, "请求路径无效")
        path = urlsplit(self.path).path
        if path == "/healthz":
            return self._send(200, "ok", "text/plain; charset=utf-8")
        if path in ("/login", "/register"):
            if user_for_session(self._session_token()):
                return self._redirect("/")
            return self._send(200, login_html(path[1:]))
        if path != "/" and path != "/index.html" and not path.startswith("/api/"):
            return self._send(404, "页面不存在")
        user = self._require_user()
        if not user:
            return
        try:
            return self._proxy(ensure_backend(user["id"]))
        except Exception:
            return self._send(503, "服务暂不可用，请稍后刷新")

    def do_POST(self):
        if not self.path.startswith("/") or self.path.startswith("//") or "\\" in self.path:
            return self._send(400, "请求路径无效")
        path = urlsplit(self.path).path
        if not self._same_origin():
            return self._reject_cross_site()
        if path in ("/auth/login", "/auth/register"):
            body = self._body(AUTH_BODY)
            if body is None:
                return
            try:
                form = parse_qs(body.decode("utf-8"), keep_blank_values=True, max_num_fields=5)
                username = form.get("username", [""])[0].strip()
                password = form.get("password", [""])[0]
                invite = form.get("invite", [""])[0]
            except (UnicodeDecodeError, ValueError):
                return self._send(400, "表单无效")
            if path.endswith("register"):
                if form.get("privacy_ack", [""])[0] != "1":
                    return self._send(400, login_html("register", "请先阅读并确认数据流转说明"))
                key = "register@" + self.client_address[0]
                ip_key = "ip@" + self.client_address[0]
                if not allowed_login_attempt(key) or not allowed_login_attempt(ip_key, 100):
                    return self._send(429, login_html("register", "尝试过于频繁，请稍后再试"))
                user_id, error = register(invite, username, password)
                if error:
                    record_failed_login(key)
                    record_failed_login(ip_key)
                    return self._send(400, login_html("register", error))
            else:
                key = username.lower() + "@" + self.client_address[0]
                ip_key = "ip@" + self.client_address[0]
                if not allowed_login_attempt(key) or not allowed_login_attempt(ip_key, 100):
                    return self._send(429, login_html("login", "尝试过于频繁，请稍后再试"))
                user_id = login(username, password)
                if not user_id:
                    record_failed_login(key)
                    record_failed_login(ip_key)
                    return self._send(401, login_html("login", "用户名或密码不正确"))
            return self._redirect("/", new_session(user_id))
        if path == "/auth/logout":
            clear_session(self._session_token())
            return self._redirect("/login", "")
        if not path.startswith("/api/"):
            return self._send(404, "页面不存在")
        user = self._require_user()
        if not user:
            return
        body = self._body(MAX_BODY)
        if body is None:
            return
        try:
            return self._proxy(ensure_backend(user["id"]), body)
        except Exception:
            return self._send(503, "服务暂不可用，请稍后刷新")

    def do_PUT(self):
        return self._mutate_other()

    def do_DELETE(self):
        return self._mutate_other()

    def _mutate_other(self):
        if not self.path.startswith("/") or self.path.startswith("//") or "\\" in self.path:
            return self._send(400, "请求路径无效")
        if not self._same_origin():
            return self._reject_cross_site()
        if not urlsplit(self.path).path.startswith("/api/"):
            return self._send(404, "页面不存在")
        user = self._require_user()
        if not user:
            return
        body = self._body(MAX_BODY)
        if body is None:
            return
        try:
            return self._proxy(ensure_backend(user["id"]), body)
        except Exception:
            return self._send(503, "服务暂不可用，请稍后刷新")


def main():
    init_db()
    if len(sys.argv) >= 2 and sys.argv[1] == "invite":
        label = sys.argv[2] if len(sys.argv) > 2 else "friend"
        print(create_invite(label))
        return
    if len(sys.argv) != 1:
        raise SystemExit("Usage: python share_gateway.py [invite [label]]")
    server = ThreadingHTTPServer((BIND, PORT), Handler)
    print(f"A股智能助手账户网关启动：{BIND}:{PORT}")
    threading.Thread(target=restore_backends, daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        stop_backends()


if __name__ == "__main__":
    main()
